/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 8192
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_EXITED,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_HARD_LIMIT_KILLED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    time_t finished_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int monitor_registered;
    int producer_joined;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    int exit_status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_ctx_t;

static volatile sig_atomic_t g_child_exited = 0;
static volatile sig_atomic_t g_shutdown = 0;
static volatile sig_atomic_t g_run_forward_stop = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static void sig_handler(int signo)
{
    if (signo == SIGCHLD)
        g_child_exited = 1;
    if (signo == SIGINT || signo == SIGTERM)
        g_shutdown = 1;
}

static void run_client_sig_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
        g_run_forward_stop = 1;
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end;
    unsigned long mib;

    end = NULL;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end;
        long nice_value;

        end = NULL;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_EXITED:
        return "exited";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

static container_record_t *find_container_by_id(container_record_t *head, const char *id)
{
    while (head) {
        if (strncmp(head->id, id, sizeof(head->id)) == 0)
            return head;
        head = head->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid(container_record_t *head, pid_t pid)
{
    while (head) {
        if (head->host_pid == pid)
            return head;
        head = head->next;
    }
    return NULL;
}

static int is_container_active(const container_record_t *rec)
{
    return rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING;
}

static int rootfs_in_use(container_record_t *head, const char *rootfs)
{
    while (head) {
        if (is_container_active(head) && strncmp(head->rootfs, rootfs, sizeof(head->rootfs)) == 0)
            return 1;
        head = head->next;
    }
    return 0;
}

static void update_container_exit(container_record_t *rec, int status)
{
    rec->finished_at = time(NULL);
    rec->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 0;
    rec->exit_signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0;

    if (WIFEXITED(status)) {
        rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
        return;
    }

    if (!WIFSIGNALED(status)) {
        rec->state = CONTAINER_KILLED;
        return;
    }

    if (rec->stop_requested) {
        rec->state = CONTAINER_STOPPED;
    } else if (WTERMSIG(status) == SIGKILL) {
        rec->state = CONTAINER_HARD_LIMIT_KILLED;
    } else {
        rec->state = CONTAINER_KILLED;
    }
}

static void join_producer_if_needed(container_record_t *rec)
{
    if (!rec->producer_joined) {
        pthread_join(rec->producer_thread, NULL);
        rec->producer_joined = 1;
    }
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx;
    log_item_t item;

    ctx = (supervisor_ctx_t *)arg;
    mkdir(LOG_DIR, 0755);

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        if (write(fd, item.data, item.length) < 0) {
            close(fd);
            continue;
        }

        close(fd);
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_ctx_t *p_ctx;

    p_ctx = (producer_ctx_t *)arg;
    while (1) {
        char buf[LOG_CHUNK_SIZE];
        ssize_t n;
        log_item_t item;

        n = read(p_ctx->read_fd, buf, sizeof(buf));
        if (n <= 0)
            break;

        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, p_ctx->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;
        memcpy(item.data, buf, (size_t)n);

        if (bounded_buffer_push(p_ctx->log_buffer, &item) != 0)
            break;
    }

    close(p_ctx->read_fd);
    free(p_ctx);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg;

    cfg = (child_config_t *)arg;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("mount private");
        return 1;
    }

    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0)
            return 1;
        if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
            return 1;
        close(cfg->log_write_fd);
    }

    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) != 0)
            perror("setpriority");
    }

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    if (mkdir("/proc", 0555) != 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("exec");
    return 1;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           container_record_t **out_rec,
                           char *err,
                           size_t err_len)
{
    int pfd[2];
    child_config_t *cfg;
    void *stack;
    pid_t pid;
    producer_ctx_t *prod;
    container_record_t *rec;

    if (find_container_by_id(ctx->containers, req->container_id) != NULL &&
        is_container_active(find_container_by_id(ctx->containers, req->container_id))) {
        snprintf(err, err_len, "Container id already running: %s", req->container_id);
        return -1;
    }

    if (rootfs_in_use(ctx->containers, req->rootfs)) {
        snprintf(err, err_len, "Rootfs already in use by another running container");
        return -1;
    }

    if (pipe(pfd) != 0) {
        snprintf(err, err_len, "pipe failed: %s", strerror(errno));
        return -1;
    }

    cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        close(pfd[0]);
        close(pfd[1]);
        snprintf(err, err_len, "out of memory");
        return -1;
    }

    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pfd[1];

    stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pfd[0]);
        close(pfd[1]);
        free(cfg);
        snprintf(err, err_len, "out of memory");
        return -1;
    }

    pid = clone(child_fn,
                (char *)stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD,
                cfg);

    close(pfd[1]);

    if (pid < 0) {
        close(pfd[0]);
        free(cfg);
        free(stack);
        snprintf(err, err_len, "clone failed: %s", strerror(errno));
        return -1;
    }

    free(cfg);
    free(stack);

    prod = calloc(1, sizeof(*prod));
    if (!prod) {
        kill(pid, SIGKILL);
        close(pfd[0]);
        snprintf(err, err_len, "out of memory");
        return -1;
    }

    prod->read_fd = pfd[0];
    prod->log_buffer = &ctx->log_buffer;
    strncpy(prod->container_id, req->container_id, sizeof(prod->container_id) - 1);

    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        kill(pid, SIGKILL);
        close(pfd[0]);
        free(prod);
        snprintf(err, err_len, "out of memory");
        return -1;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    strncpy(rec->command, req->command, sizeof(rec->command) - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);

    if (pthread_create(&rec->producer_thread, NULL, producer_thread, prod) != 0) {
        kill(pid, SIGKILL);
        close(pfd[0]);
        free(prod);
        free(rec);
        snprintf(err, err_len, "failed to create producer thread");
        return -1;
    }

    rec->next = ctx->containers;
    ctx->containers = rec;

    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd,
                              rec->id,
                              rec->host_pid,
                              rec->soft_limit_bytes,
                              rec->hard_limit_bytes) == 0) {
        rec->monitor_registered = 1;
    }

    *out_rec = rec;
    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_pid(ctx->containers, pid);
        if (rec) {
            pthread_t producer;
            int need_join;

            update_container_exit(rec, status);
            if (rec->monitor_registered && ctx->monitor_fd >= 0) {
                unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);
                rec->monitor_registered = 0;
            }

            producer = rec->producer_thread;
            need_join = !rec->producer_joined;
            rec->producer_joined = 1;
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (need_join)
                pthread_join(producer, NULL);
        } else {
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
    }
}

static void fill_ps_response(supervisor_ctx_t *ctx, control_response_t *res)
{
    container_record_t *curr;
    size_t used;

    used = (size_t)snprintf(res->message,
                            sizeof(res->message),
                            "ID\tPID\tSTATE\tSTART\tSOFT_MIB\tHARD_MIB\tEXIT\n");

    curr = ctx->containers;
    while (curr && used < sizeof(res->message) - 1) {
        int soft_mib;
        int hard_mib;
        char line[512];
        int line_len;

        soft_mib = (int)(curr->soft_limit_bytes >> 20);
        hard_mib = (int)(curr->hard_limit_bytes >> 20);

        if (curr->state == CONTAINER_EXITED ||
            curr->state == CONTAINER_STOPPED ||
            curr->state == CONTAINER_KILLED ||
            curr->state == CONTAINER_HARD_LIMIT_KILLED) {
            if (curr->exit_signal != 0) {
                line_len = snprintf(line,
                                    sizeof(line),
                                    "%s\t%d\t%s\t%ld\t%d\t%d\tsignal=%d\n",
                                    curr->id,
                                    curr->host_pid,
                                    state_to_string(curr->state),
                                    (long)curr->started_at,
                                    soft_mib,
                                    hard_mib,
                                    curr->exit_signal);
            } else {
                line_len = snprintf(line,
                                    sizeof(line),
                                    "%s\t%d\t%s\t%ld\t%d\t%d\texit=%d\n",
                                    curr->id,
                                    curr->host_pid,
                                    state_to_string(curr->state),
                                    (long)curr->started_at,
                                    soft_mib,
                                    hard_mib,
                                    curr->exit_code);
            }
        } else {
            line_len = snprintf(line,
                                sizeof(line),
                                "%s\t%d\t%s\t%ld\t%d\t%d\t-\n",
                                curr->id,
                                curr->host_pid,
                                state_to_string(curr->state),
                                (long)curr->started_at,
                                soft_mib,
                                hard_mib);
        }

        if (line_len < 0)
            break;
        if (used + (size_t)line_len >= sizeof(res->message))
            break;

        memcpy(res->message + used, line, (size_t)line_len);
        used += (size_t)line_len;
        res->message[used] = '\0';
        curr = curr->next;
    }
}

static int read_log_file(const char *id, char *out, size_t out_len)
{
    char path[PATH_MAX];
    int fd;
    ssize_t n;
    size_t used;

    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, id);
    fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    used = 0;
    while (used < out_len - 1) {
        n = read(fd, out + used, out_len - 1 - used);
        if (n <= 0)
            break;
        used += (size_t)n;
    }
    out[used] = '\0';
    close(fd);

    return 0;
}

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t res;

    memset(&req, 0, sizeof(req));
    memset(&res, 0, sizeof(res));
    res.status = 1;
    res.exit_status = 1;

    if (read(client_fd, &req, sizeof(req)) <= 0) {
        close(client_fd);
        return;
    }

    if (req.kind == CMD_START || req.kind == CMD_RUN) {
        container_record_t *rec;
        char err[256];

        memset(err, 0, sizeof(err));

        pthread_mutex_lock(&ctx->metadata_lock);
        if (start_container(ctx, &req, &rec, err, sizeof(err)) != 0) {
            snprintf(res.message, sizeof(res.message), "%s", err);
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (write(client_fd, &res, sizeof(res)) < 0) {
                close(client_fd);
                return;
            }
            close(client_fd);
            return;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (req.kind == CMD_START) {
            res.status = 0;
            res.exit_status = 0;
            snprintf(res.message,
                     sizeof(res.message),
                     "Started container %s with PID %d",
                     rec->id,
                     rec->host_pid);
            if (write(client_fd, &res, sizeof(res)) < 0) {
                close(client_fd);
                return;
            }
            close(client_fd);
            return;
        }

        while (1) {
            int status;
            pid_t waited;

            waited = waitpid(rec->host_pid, &status, 0);
            if (waited == rec->host_pid) {
                pthread_mutex_lock(&ctx->metadata_lock);
                update_container_exit(rec, status);
                if (rec->monitor_registered && ctx->monitor_fd >= 0) {
                    unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);
                    rec->monitor_registered = 0;
                }
                pthread_mutex_unlock(&ctx->metadata_lock);

                join_producer_if_needed(rec);

                res.status = 0;
                if (WIFEXITED(status)) {
                    res.exit_status = WEXITSTATUS(status);
                    snprintf(res.message,
                             sizeof(res.message),
                             "Container %s exited with code %d",
                             rec->id,
                             WEXITSTATUS(status));
                } else if (WIFSIGNALED(status)) {
                    res.exit_status = 128 + WTERMSIG(status);
                    snprintf(res.message,
                             sizeof(res.message),
                             "Container %s terminated by signal %d",
                             rec->id,
                             WTERMSIG(status));
                } else {
                    res.exit_status = 1;
                    snprintf(res.message,
                             sizeof(res.message),
                             "Container %s terminated (unknown)",
                             rec->id);
                }
                break;
            }

            if (waited < 0 && errno == EINTR)
                continue;

            snprintf(res.message, sizeof(res.message), "waitpid failed: %s", strerror(errno));
            break;
        }

        if (write(client_fd, &res, sizeof(res)) < 0) {
            close(client_fd);
            return;
        }
        close(client_fd);
        return;
    }

    if (req.kind == CMD_PS) {
        pthread_mutex_lock(&ctx->metadata_lock);
        fill_ps_response(ctx, &res);
        pthread_mutex_unlock(&ctx->metadata_lock);
        res.status = 0;
        res.exit_status = 0;

        if (write(client_fd, &res, sizeof(res)) < 0) {
            close(client_fd);
            return;
        }
        close(client_fd);
        return;
    }

    if (req.kind == CMD_STOP) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id(ctx->containers, req.container_id);
        if (!rec || !is_container_active(rec)) {
            snprintf(res.message, sizeof(res.message), "Container not running: %s", req.container_id);
            pthread_mutex_unlock(&ctx->metadata_lock);
        } else {
            rec->stop_requested = 1;
            if (kill(rec->host_pid, SIGTERM) != 0) {
                snprintf(res.message, sizeof(res.message), "Failed to stop %s: %s", rec->id, strerror(errno));
                pthread_mutex_unlock(&ctx->metadata_lock);
            } else {
                snprintf(res.message, sizeof(res.message), "Stop requested for %s", rec->id);
                res.status = 0;
                res.exit_status = 0;
                pthread_mutex_unlock(&ctx->metadata_lock);
            }
        }

        if (write(client_fd, &res, sizeof(res)) < 0) {
            close(client_fd);
            return;
        }
        close(client_fd);
        return;
    }

    if (req.kind == CMD_LOGS) {
        char contents[CONTROL_MESSAGE_LEN - 128];

        memset(contents, 0, sizeof(contents));
        if (read_log_file(req.container_id, contents, sizeof(contents)) != 0) {
            snprintf(res.message,
                     sizeof(res.message),
                     "No logs found at %s/%s.log",
                     LOG_DIR,
                     req.container_id);
        } else if (contents[0] == '\0') {
            snprintf(res.message,
                     sizeof(res.message),
                     "Log file exists but is empty: %s/%s.log",
                     LOG_DIR,
                     req.container_id);
            res.status = 0;
            res.exit_status = 0;
        } else {
            snprintf(res.message,
                     sizeof(res.message),
                     "--- %s/%s.log ---\n%s",
                     LOG_DIR,
                     req.container_id,
                     contents);
            res.status = 0;
            res.exit_status = 0;
        }

        if (write(client_fd, &res, sizeof(res)) < 0) {
            close(client_fd);
            return;
        }
        close(client_fd);
        return;
    }

    snprintf(res.message, sizeof(res.message), "Unknown command");
    if (write(client_fd, &res, sizeof(res)) < 0) {
        close(client_fd);
        return;
    }
    close(client_fd);
}

static void shutdown_running_containers(supervisor_ctx_t *ctx)
{
    container_record_t *curr;

    pthread_mutex_lock(&ctx->metadata_lock);
    curr = ctx->containers;
    while (curr) {
        if (is_container_active(curr)) {
            curr->stop_requested = 1;
            kill(curr->host_pid, SIGTERM);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    while (1) {
        int status;
        pid_t pid;
        container_record_t *rec;

        pid = waitpid(-1, &status, 0);
        if (pid < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_pid(ctx->containers, pid);
        if (rec) {
            update_container_exit(rec, status);
            if (rec->monitor_registered && ctx->monitor_fd >= 0) {
                unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);
                rec->monitor_registered = 0;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (rec)
            join_producer_if_needed(rec);
    }
}

static void free_container_records(supervisor_ctx_t *ctx)
{
    container_record_t *curr;

    curr = ctx->containers;
    while (curr) {
        container_record_t *next;

        next = curr->next;
        if (!curr->producer_joined)
            pthread_join(curr->producer_thread, NULL);
        free(curr);
        curr = next;
    }
    ctx->containers = NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sigaction sa;
    struct sockaddr_un addr;

    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0)
        return 1;

    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    printf("Supervisor running. Control socket: %s\n", CONTROL_PATH);

    while (!g_shutdown) {
        fd_set read_fds;
        struct timeval tv;
        int rc;

        if (g_child_exited) {
            g_child_exited = 0;
            reap_children(&ctx);
        }

        FD_ZERO(&read_fds);
        FD_SET(ctx.server_fd, &read_fds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &read_fds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (rc == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &read_fds)) {
            int client_fd;

            client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0)
                continue;

            handle_client(&ctx, client_fd);
        }
    }

    shutdown_running_containers(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    free_container_records(&ctx);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    return 0;
}

static int request_over_socket(const control_request_t *req, control_response_t *res)
{
    int sock;
    struct sockaddr_un addr;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sock);
        return 1;
    }

    if (read(sock, res, sizeof(*res)) <= 0) {
        perror("read");
        close(sock);
        return 1;
    }

    close(sock);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    control_response_t res;

    memset(&res, 0, sizeof(res));
    if (request_over_socket(req, &res) != 0)
        return 1;

    printf("%s\n", res.message);
    if (res.status != 0)
        return res.status;
    return res.exit_status;
}

static int request_stop_by_id(const char *id)
{
    control_request_t stop_req;
    control_response_t stop_res;

    memset(&stop_req, 0, sizeof(stop_req));
    memset(&stop_res, 0, sizeof(stop_res));

    stop_req.kind = CMD_STOP;
    strncpy(stop_req.container_id, id, sizeof(stop_req.container_id) - 1);

    if (request_over_socket(&stop_req, &stop_res) != 0)
        return 1;

    fprintf(stderr, "%s\n", stop_res.message);
    return stop_res.status;
}

static int send_run_request_with_signal_forwarding(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction sa;
    control_response_t res;
    int stop_sent;

    memset(&old_int, 0, sizeof(old_int));
    memset(&old_term, 0, sizeof(old_term));
    memset(&sa, 0, sizeof(sa));
    memset(&res, 0, sizeof(res));

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sock);
        return 1;
    }

    g_run_forward_stop = 0;
    sa.sa_handler = run_client_sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, &old_int);
    sigaction(SIGTERM, &sa, &old_term);

    stop_sent = 0;
    while (1) {
        ssize_t n;

        n = read(sock, &res, sizeof(res));
        if (n > 0)
            break;

        if (n < 0 && errno == EINTR) {
            if (g_run_forward_stop && !stop_sent) {
                request_stop_by_id(req->container_id);
                stop_sent = 1;
            }
            continue;
        }

        fprintf(stderr, "Supervisor closed connection unexpectedly\n");
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
        close(sock);
        return 1;
    }

    sigaction(SIGINT, &old_int, NULL);
    sigaction(SIGTERM, &old_term, NULL);

    close(sock);
    printf("%s\n", res.message);
    if (res.status != 0)
        return res.status;
    return res.exit_status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_run_request_with_signal_forwarding(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

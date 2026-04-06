/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
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
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
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
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
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
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

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

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

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
        char *end = NULL;
        long nice_value;

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
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
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

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
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

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
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

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    mkdir(LOG_DIR, 0755);
    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            if(write(fd, item.data, item.length) < 0) { /* ignore */ }
            close(fd);
        }
    }
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (cfg->log_write_fd > 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }
    if (cfg->nice_value != 0) {
        if(nice(cfg->nice_value) < 0) { /* ignore */ };
    }

    if (chroot(cfg->rootfs) != 0) return 1;
    if (chdir("/") != 0) return 1;

    mount("proc", "/proc", "proc", 0, NULL);
    char *exec_args[] = { "sh", "-c", cfg->command, NULL };
    execvp("/bin/sh", exec_args);
    return 1;
}

int register_with_monitor(int monitor_fd,
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

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static volatile sig_atomic_t g_child_exited = 0;
static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int signo) {
    if (signo == SIGCHLD) g_child_exited = 1;
    if (signo == SIGINT || signo == SIGTERM) g_shutdown = 1;
}

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_ctx_t;

static void *producer_thread(void *arg) {
    producer_ctx_t *p_ctx = (producer_ctx_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;
    while ((n = read(p_ctx->read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, p_ctx->container_id, CONTAINER_ID_LEN - 1);
        item.length = n;
        memcpy(item.data, buf, n);
        bounded_buffer_push(p_ctx->log_buffer, &item);
    }
    close(p_ctx->read_fd);
    free(p_ctx);
    return NULL;
}

static void handle_client(supervisor_ctx_t *ctx, int client_fd) {
    control_request_t req;
    control_response_t res;
    memset(&res, 0, sizeof(res));
    if (read(client_fd, &req, sizeof(req)) <= 0) { close(client_fd); return; }
    
    pthread_mutex_lock(&ctx->metadata_lock);
    if (req.kind == CMD_START || req.kind == CMD_RUN) {
        int pfd[2];
        if (pipe(pfd) == 0) {
            child_config_t *cfg = malloc(sizeof(child_config_t));
            strncpy(cfg->id, req.container_id, CONTAINER_ID_LEN - 1);
            strncpy(cfg->rootfs, req.rootfs, PATH_MAX - 1);
            strncpy(cfg->command, req.command, CHILD_COMMAND_LEN - 1);
            cfg->nice_value = req.nice_value;
            cfg->log_write_fd = pfd[1];
            void *stack = malloc(STACK_SIZE);
            pid_t pid = clone(child_fn, (char*)stack + STACK_SIZE, CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD, cfg);
            close(pfd[1]);
            if (pid > 0) {
                producer_ctx_t *p_ctx = malloc(sizeof(producer_ctx_t));
                p_ctx->read_fd = pfd[0];
                p_ctx->log_buffer = &ctx->log_buffer;
                strncpy(p_ctx->container_id, req.container_id, CONTAINER_ID_LEN - 1);
                pthread_t pt;
                pthread_create(&pt, NULL, producer_thread, p_ctx);
                pthread_detach(pt);
                
                container_record_t *rec = malloc(sizeof(*rec));
                memset(rec, 0, sizeof(*rec));
                strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
                rec->host_pid = pid;
                rec->started_at = time(NULL);
                rec->state = CONTAINER_RUNNING;
                rec->soft_limit_bytes = req.soft_limit_bytes;
                rec->hard_limit_bytes = req.hard_limit_bytes;
                snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, rec->id);
                rec->next = ctx->containers;
                ctx->containers = rec;
                
                if (ctx->monitor_fd >= 0) register_with_monitor(ctx->monitor_fd, rec->id, pid, rec->soft_limit_bytes, rec->hard_limit_bytes);
                snprintf(res.message, sizeof(res.message), "Started container %s with PID %d", req.container_id, pid);
                res.status = 0;
            } else { res.status = 1; strcpy(res.message, "clone failed."); free(stack); close(pfd[0]); }
        }
    } else if (req.kind == CMD_PS) {
        char buf[2048] = "ID\tPID\tSTATE\n";
        container_record_t *curr = ctx->containers;
        while (curr) {
            char line[256];
            snprintf(line, sizeof(line), "%s\t%d\t%s\n", curr->id, curr->host_pid, state_to_string(curr->state));
            strncat(buf, line, sizeof(buf) - strlen(buf) - 1);
            curr = curr->next;
        }
        strncpy(res.message, buf, sizeof(res.message) - 1);
        res.status = 0;
    } else if (req.kind == CMD_STOP) {
        container_record_t *curr = ctx->containers;
        while (curr) {
            if (strcmp(curr->id, req.container_id) == 0 && curr->state == CONTAINER_RUNNING) {
                kill(curr->host_pid, SIGTERM);
                curr->state = CONTAINER_STOPPED;
                snprintf(res.message, sizeof(res.message), "Stopped %s", req.container_id);
                res.status = 0; break;
            }
            curr = curr->next;
        }
        if (!curr) { strcpy(res.message, "Not found"); res.status = 1; }
    } else if (req.kind == CMD_LOGS) {
        snprintf(res.message, sizeof(res.message), "Logs are written to logs/%s.log", req.container_id);
        res.status = 0;
    } else {
        strcpy(res.message, "Unknown cmd"); res.status = 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    if (write(client_fd, &res, sizeof(res)) < 0) { /* ignore */ }
    close(client_fd);
}

static int run_supervisor(const char *rootfs)
{
    (void)rootfs;
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) return 1;
    if (bounded_buffer_init(&ctx.log_buffer) != 0) return 1;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(ctx.server_fd, 5) < 0) {
        perror("bind or listen");
        return 1;
    }

    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);

    fd_set read_fds;
    printf("Supervisor running and ready to accept commands...\n");
    while (!g_shutdown) {
        if (g_child_exited) {
            g_child_exited = 0;
            pthread_mutex_lock(&ctx.metadata_lock);
            int status;
            pid_t pid;
            while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
                container_record_t *curr = ctx.containers;
                while (curr) {
                    if (curr->host_pid == pid) {
                        curr->state = CONTAINER_EXITED;
                        curr->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 0;
                        curr->exit_signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
                        if (ctx.monitor_fd >= 0) unregister_from_monitor(ctx.monitor_fd, curr->id, pid);
                        break;
                    }
                    curr = curr->next;
                }
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        
        FD_ZERO(&read_fds);
        FD_SET(ctx.server_fd, &read_fds);
        struct timeval tv = {1, 0};
        
        if (select(ctx.server_fd + 1, &read_fds, NULL, NULL, &tv) > 0) {
            int client = accept(ctx.server_fd, NULL, NULL);
            if (client >= 0) handle_client(&ctx, client);
        }
    }

    printf("\nShutting down...\n");
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Could not connect to supervisor socket");
        close(sock); return 1;
    }

    if (write(sock, req, sizeof(*req)) != sizeof(*req)) {
        perror("Write error to supervisor");
        close(sock); return 1;
    }

    control_response_t res;
    if (read(sock, &res, sizeof(res)) > 0) {
        printf("%s\n", res.message);
    } else {
        printf("Supervisor closed connection.\n");
    }

    close(sock);
    return res.status;
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

    return send_control_request(&req);
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

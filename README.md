# Multi-Container Runtime with Kernel Memory Monitor

This repository contains a supervised multi-container runtime in C and a Linux kernel module for per-container memory monitoring.

## 1. Team Information

- Team Member 1: <Name>, <SRN>
- Team Member 2: <Name>, <SRN>

## 2. Build, Load, and Run Instructions

The commands below assume you are in the [boilerplate/](boilerplate/) directory.

### 2.1 Prerequisites (Ubuntu VM)

This project is designed for Ubuntu 22.04 or 24.04 in a VM (Secure Boot off, no WSL).

```bash
sudo apt update
sudo apt install -y make build-essential linux-headers-$(uname -r)
```

### 2.2 Build

```bash
cd boilerplate
make
```

CI-safe compile check:

```bash
make ci
```

### 2.3 Environment Check

```bash
chmod +x environment-check.sh
sudo ./environment-check.sh
```

### 2.4 Prepare Rootfs

```bash
mkdir -p rootfs-base
wget -O alpine.tar.gz https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine.tar.gz -C rootfs-base

cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

Copy workloads into container rootfs if you want to run them inside containers:

```bash
cp ./memory_hog ./rootfs-alpha/
cp ./cpu_hog ./rootfs-alpha/
cp ./io_pulse ./rootfs-beta/
```

### 2.5 Load Kernel Module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

### 2.6 Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### 2.7 CLI Commands (from another terminal)

Start background containers:

```bash
cd boilerplate
sudo ./engine start alpha ./rootfs-alpha "/bin/sh"
sudo ./engine start beta ./rootfs-beta "/bin/sh"
```

Run foreground container (returns exit status):

```bash
sudo ./engine run gamma ./rootfs-alpha "/cpu_hog 8" --nice 5
```

List metadata:

```bash
sudo ./engine ps
```

Show logs:

```bash
sudo ./engine logs alpha
```

Stop container:

```bash
sudo ./engine stop alpha
```

### 2.8 Memory-Limit Demo

Soft/hard example:

```bash
sudo ./engine run memtest ./rootfs-alpha "/memory_hog 8 250" --soft-mib 48 --hard-mib 80
```

Inspect monitor output:

```bash
dmesg | tail -n 100
```

Expected behavior:
- First threshold crossing prints SOFT LIMIT warning.
- Hard-limit crossing leads to SIGKILL and runtime reports hard_limit_killed in metadata.

### 2.9 Scheduling Experiments

Experiment A: CPU vs CPU with different nice values.

```bash
sudo ./engine start cpu_hi ./rootfs-alpha "/cpu_hog 12" --nice 0
sudo ./engine start cpu_lo ./rootfs-beta "/cpu_hog 12" --nice 10
sudo ./engine ps
```

Experiment B: CPU vs I/O concurrently.

```bash
sudo ./engine start cpu_mix ./rootfs-alpha "/cpu_hog 12" --nice 0
sudo ./engine start io_mix ./rootfs-beta "/io_pulse 40 100" --nice 0
sudo ./engine ps
```

Compare timestamps and progress cadence from:

```bash
sudo ./engine logs cpu_hi
sudo ./engine logs cpu_lo
sudo ./engine logs cpu_mix
sudo ./engine logs io_mix
```

### 2.10 Teardown and Cleanup

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
sudo ./engine stop cpu_hi
sudo ./engine stop cpu_lo
sudo ./engine stop cpu_mix
sudo ./engine stop io_mix

ps -eo pid,ppid,state,cmd | grep engine
ps -eo pid,ppid,state,cmd | grep defunct

sudo rmmod monitor
make clean
```

## 3. Demo with Screenshots

Add screenshots with brief captions for each required item.

1. Multi-container supervision: 2+ containers under one supervisor.
2. Metadata tracking: output of engine ps with state and limits.
3. Bounded-buffer logging: per-container log contents from engine logs.
4. CLI and IPC: command request and response via control channel.
5. Soft-limit warning: dmesg entry showing SOFT LIMIT.
6. Hard-limit enforcement: dmesg hard-limit kill plus ps state hard_limit_killed.
7. Scheduling experiment: measurable difference from two configurations.
8. Clean teardown: no zombies, stopped containers, graceful supervisor shutdown.

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

Each container process is created with clone using PID, UTS, and mount namespaces. PID namespace gives each container its own process-ID view; UTS namespace isolates hostname; mount namespace isolates mount operations like proc mount. The runtime then chroots into a per-container writable rootfs copy, so each container sees its assigned filesystem as /. The kernel remains shared for all containers, so scheduling, global memory accounting, and kernel code are common even though process and filesystem views are isolated.

### 4.2 Supervisor and Process Lifecycle

A long-running supervisor centralizes lifecycle control and metadata: start time, host PID, state transitions, limits, and exit reason. This allows asynchronous start, stop, ps, and logs commands while reaping children correctly. SIGCHLD handling prevents zombie buildup by collecting child exit status via waitpid. Signal flow is explicit: a stop command marks stop_requested and sends SIGTERM, then final classification distinguishes manual stop from hard-limit kill.

### 4.3 IPC, Threads, and Synchronization

Two IPC paths are used.

- Control plane: UNIX domain socket between CLI clients and supervisor.
- Logging plane: per-container stdout/stderr pipes into producer threads.

Logging uses a bounded producer-consumer buffer with one mutex and two condition variables. Without synchronization, producers and consumer could race on head/tail/count, corrupt buffer indices, or drop entries during concurrent writes. The shutdown flag and condition broadcasts avoid deadlock and allow draining remaining log entries before logger exit.

Metadata is protected by a separate mutex to decouple lifecycle state from log-buffer lock scope.

### 4.4 Memory Management and Enforcement

RSS measures resident physical pages currently mapped by a process; it does not fully capture all memory pressure sources (for example, kernel memory not charged to that process). Soft and hard limits represent two different policies: soft limit is observability and warning, hard limit is enforcement. Enforcement is implemented in kernel space because only kernel context can reliably inspect live task memory state and issue authoritative termination independent of user-space race conditions or supervisor stalls.

### 4.5 Scheduling Behavior

The experiments show CFS preference effects from nice levels and workload type interactions.

- CPU vs CPU with different nice: the lower nice value receives a larger CPU share, finishing earlier and producing denser progress output.
- CPU vs I/O: I/O-bound workload remains responsive due to frequent sleep/wakeup behavior while CPU-bound workload consumes leftover CPU time.

This reflects scheduler goals: fairness weighted by priority, good responsiveness for interactive/sleep-heavy tasks, and strong throughput for compute tasks when no contention exists.

## 5. Design Decisions and Tradeoffs

### Namespace and Rootfs Isolation

- Choice: CLONE_NEWPID + CLONE_NEWUTS + CLONE_NEWNS with chroot per container.
- Tradeoff: chroot is simpler than pivot_root but offers weaker escape resistance in misconfigured environments.
- Justification: clear educational path and straightforward debugging for this project scope.

### Supervisor Architecture

- Choice: single long-running parent supervisor with centralized metadata list.
- Tradeoff: serialized command handling can reduce throughput under many concurrent clients.
- Justification: simpler correctness model for lifecycle and signal handling.

### IPC and Logging Pipeline

- Choice: UNIX socket for control and pipe plus bounded buffer for logs.
- Tradeoff: message-size limits require truncation strategy for very large responses.
- Justification: separation of concerns and explicit demonstration of two IPC mechanisms.

### Kernel Monitor

- Choice: timer-based periodic RSS checks with linked-list registration.
- Tradeoff: periodic checks are not instantaneous; enforcement latency is up to timer period.
- Justification: low complexity and deterministic behavior for lab demonstration.

### Scheduler Experiments

- Choice: use cpu_hog and io_pulse with varying nice values.
- Tradeoff: limited workload diversity compared to full benchmark suites.
- Justification: repeatable, interpretable signals tied directly to scheduler policy.

## 6. Scheduler Experiment Results

Record your actual measurements from your VM in this section before submission.

Example table format:

| Experiment | Workload A | Workload B | Config | Observation |
| --- | --- | --- | --- | --- |
| A1 | cpu_hog 12s | cpu_hog 12s | nice 0 vs nice 10 | nice 0 completes earlier, emits more progress lines per wall-time |
| B1 | cpu_hog 12s | io_pulse 40x100ms | both nice 0 | io_pulse stays responsive while cpu_hog consumes spare CPU |

Suggested raw evidence to include:

```bash
# capture timestamps around run
/usr/bin/time -f "%E" sudo ./engine run exp1 ./rootfs-alpha "/cpu_hog 12" --nice 0
/usr/bin/time -f "%E" sudo ./engine run exp2 ./rootfs-beta "/cpu_hog 12" --nice 10

# compare log progression
sudo ./engine logs exp1
sudo ./engine logs exp2
```

## 7. Source Files

The required source artifacts are present in [boilerplate/](boilerplate/):

- [boilerplate/engine.c](boilerplate/engine.c)
- [boilerplate/monitor.c](boilerplate/monitor.c)
- [boilerplate/monitor_ioctl.h](boilerplate/monitor_ioctl.h)
- [boilerplate/cpu_hog.c](boilerplate/cpu_hog.c)
- [boilerplate/io_pulse.c](boilerplate/io_pulse.c)
- [boilerplate/memory_hog.c](boilerplate/memory_hog.c)
- [boilerplate/Makefile](boilerplate/Makefile)

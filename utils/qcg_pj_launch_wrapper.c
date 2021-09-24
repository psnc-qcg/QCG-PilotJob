#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/timeb.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>


#define DEBUG 0

void date_time(struct timespec *ts) {
	clock_gettime(CLOCK_REALTIME, ts);
}


int main(int argc, char **argv) {
	pid_t child_pid;
	int child_status;
    int child_exitcode;
	char id_buffer[128];
	struct timespec tstart, tfinish;
	char *reporter_id = NULL;
	int debug_v = DEBUG;

	if (getenv("QCG_PJ_WRAPPER_DEBUG") != NULL && strncmp(getenv("QCG_PJ_WRAPPER_DEBUG"), "0", 2))
		debug_v = 1;

	if (argc < 2) {
		fprintf(stderr, "error: missing arguments\n");
		exit(-1);
	}

	date_time(&tstart);

	if ((child_pid = fork()) == -1) {
		perror("fork error");
		exit(-1);
	} else if (child_pid == 0) {
		execvp(argv[1], &argv[1]);
		fprintf(stderr, "error: '%s' command unknown\n", argv[1]);
		exit(-1);
	} else {
		char *report_path;
		FILE *out_fd;
		ssize_t written;

		if (waitpid(child_pid, &child_status, 0) < 0) {
		    perror("failed to wait for a child process");
        }

        child_exitcode = WEXITSTATUS(child_status);
		signal(SIGPIPE, SIG_IGN);

		if (debug_v) {
			fprintf(stderr, "child job finished with %d exit code\n", child_exitcode);
		}

		date_time(&tfinish);

		reporter_id = getenv("QCG_PM_RUNTIME_STATS_ID");
		if (reporter_id == NULL) {
			snprintf(id_buffer, sizeof(id_buffer), "pid:%d", getpid());
			reporter_id = id_buffer;
		}
			
		report_path = getenv("QCG_PM_RUNTIME_STATS_PIPE");
		if (report_path == NULL) {
			out_fd = stderr;
		} else {
			out_fd = fopen(report_path, "w");
			if (out_fd == NULL) {
				perror("failed to open pipe file");
				exit(-2);
			}
		}

		if (debug_v) {
			fprintf(stderr, "runtime stats id: %s\n", reporter_id);
			fprintf(stderr, "runtime stats path: %s\n", report_path);
			fprintf(stderr, "%s,%lf,%lf\n", reporter_id, tstart.tv_sec + (double)tstart.tv_nsec / 1e9, tfinish.tv_sec + (double)tfinish.tv_nsec / 1e9);
		}

		written = fprintf(out_fd, "%s,%lf,%lf\n", reporter_id, tstart.tv_sec + (double)tstart.tv_nsec / 1e9, tfinish.tv_sec + (double)tfinish.tv_nsec / 1e9);
		if (written < 0) {
			perror("write data to pipe error");
		} else {
			if (debug_v) {
				fprintf(stderr, "wrote %ld bytes\n", written);
			}
        }

		if (out_fd != stderr) {
			if (fclose(out_fd) != 0) {
				perror("failed to close write side of pipe");
			}
		}

		exit(child_exitcode);
	}

	return 0;
}

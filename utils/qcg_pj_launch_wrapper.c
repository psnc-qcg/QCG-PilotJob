#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/timeb.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>


#define DEBUG 0

void date_time(struct timespec *ts) {
	clock_gettime(CLOCK_REALTIME, ts);
}

char* find_command_path(const char *command) {
	char buffer[1024];
	char *path_env = getenv("PATH");
	char *token;

	token = strtok(path_env, ":");
	while (token) {
		strncpy(buffer, token, sizeof(buffer));
		strncat(buffer, "/", sizeof(buffer));
		strncat(buffer, command, sizeof(buffer));

#if DEBUG
		printf("checking path [%s] ...\n", buffer);
#endif
		if (!access(buffer, X_OK)) {
			printf("found command's path as [%s]\n", buffer);
			return strdup(buffer);
		}

		token = strtok(NULL, ":");
	}

	return NULL;
}


int main(int argc, char **argv) {
	pid_t child_pid;
	int child_status;
	char *command = NULL;
	char id_buffer[128];
	struct timespec tstart, tfinish;
	char *reporter_id = NULL;

	if (argc < 2) {
		fprintf(stderr, "error: missing arguments\n");
		exit(-1);
	}

	command = argv[1];
	if (access(command, X_OK)) {
		command = find_command_path(argv[1]);
		if (! command) {
			fprintf(stderr, "error: command %s not found\n", argv[1]);
			exit(-1);
		}
	}

	date_time(&tstart);

	if ((child_pid = fork()) == -1) {
		perror("fork error");
		exit(-1);
	} else if (child_pid == 0) {
		execv(command, &argv[1]);
		fprintf(stderr, "error: '%s' command unknown\n", argv[1]);
		exit(-1);
	} else {
		char *report_path;
		FILE *out_fd;
		ssize_t written;

		waitpid(child_pid, &child_status, 0);

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
		}
#if DEBUG
		fprintf(stderr, "runtime stats id: %s\n", reporter_id);
		fprintf(stderr, "runtime stats path: %s\n", report_path);
		fprintf(stderr, "%s,%lf,%lf\n", reporter_id, tstart.tv_sec + (double)tstart.tv_nsec / 1e9, tfinish.tv_sec + (double)tfinish.tv_nsec / 1e9);
#endif

		written = fprintf(out_fd, "%s,%lf,%lf\n", reporter_id, tstart.tv_sec + (double)tstart.tv_nsec / 1e9, tfinish.tv_sec + (double)tfinish.tv_nsec / 1e9);
		if (written < 0) {
			perror("write error");
		}

#if DEBUG
		fprintf(stderr, "wroute %ld bytes\n", written);
#endif

		if (out_fd != stderr)
			fclose(out_fd);

		exit(WEXITSTATUS(child_status));
	}

	return 0;
}

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// int run_pthread_demo(int argc, char* argv[]);
int run_prod_cons(int argc, char* argv[]);
// int run_char_stat(int argc, char* argv[]);

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr,
            "Usage:\n"
            "  %s pthread [args...]\n"
            "  %s prodcons <file> [#producers] [#consumers]\n"
            "  %s stat <file>\n",
            argv[0], argv[0], argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "pthread") == 0)
        return run_prod_cons(argc - 1, argv + 1);
    else if (strcmp(argv[1], "prodcons") == 0)
        return run_prod_cons(argc - 1, argv + 1);
    else if (strcmp(argv[1], "stat") == 0)
        return run_prod_cons(argc - 1, argv + 1);
    else {
        fprintf(stderr, "Unknown subcommand: %s\n", argv[1]);
        return 1;
    }
}
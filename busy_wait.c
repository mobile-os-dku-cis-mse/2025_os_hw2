// busy-wait를 사용한 방법


#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>   // clock_gettime

typedef struct sharedobject {
    FILE *rfile;
    int linenum;
    char *line;
    pthread_mutex_t lock;
    int full;
} so_t;

int wait = 1;
int end = 0;
int not = 0;
int read_line = 0;
int write_line = 0;

void *producer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    FILE *rfile = so->rfile;
    int i = 0;
    char *line = NULL;
    size_t len = 0;

    int quit = 0; // EOF 센티넬 보낸 뒤 바깥 while 종료

    while (!quit) {
        while (!wait && !end) {}              // 바쁜 대기(기존 설계 유지)
        pthread_mutex_lock(&so->lock);

        if (so->full == 1 && !end) {
            wait = 0;
            pthread_mutex_unlock(&so->lock);
            continue;
        }

        // === 여기부터: 10줄 배치 만들기 ===
        char *batch = NULL;
        size_t bcap = 0, blen = 0;
        int    got  = 0;

        for (int k = 0; k < 4611; k++) {
            ssize_t n = getdelim(&line, &len, '\n', rfile);
            if (n == -1) {
                if (got == 0) {
                    // 이 배치에서 한 줄도 못 읽음 → 진짜 EOF: 센티넬 전송 후 종료
                    so->full = 1;
                    so->line = NULL;      // 소비자에게 EOF 알림
                    wait = 0;
                    end  = 1;
                    pthread_mutex_unlock(&so->lock);
                    quit = 1;
                }
                // got > 0 이면 아래에서 부분 배치 전송
                break;
            }

            // batch에 이어붙이기
            if (blen + (size_t)n + 1 > bcap) {
                bcap = (blen + (size_t)n + 1) * 2;
                char *nb = realloc(batch, bcap);
                if (!nb) { free(batch); pthread_mutex_unlock(&so->lock); perror("realloc"); pthread_exit(NULL); }
                batch = nb;
            }
            memcpy(batch + blen, line, (size_t)n);
            blen += (size_t)n;
            batch[blen] = '\0';
            got++;
        }

        if (quit) break;                  // EOF 센티넬을 이미 보냈음

        // got >= 1이면 배치 전송
        if (got >= 1) {
            so->linenum = i;              // 배치의 첫 줄 번호
            i += got;                     // 누적 줄 번호 증가
            so->line = batch;             // 소비자에게 소유권 넘김 (소비자가 free)
            so->full = 1;
            wait = 0;
            pthread_mutex_unlock(&so->lock);
        } else {
            // 이 경우는 드묾(예외 경로 안전장치)
            pthread_mutex_unlock(&so->lock);
            free(batch);
        }
    }

    free(line);
   // printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), i);
    *ret = i;
    read_line += i;
    pthread_exit(ret);
}

void *consumer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    int i = 0;
    char *line;

    while (1) {
        while (wait && !not) {}
        pthread_mutex_lock(&so->lock);

        if (so->full == 0 && !not) {
            wait = 1;
            pthread_mutex_unlock(&so->lock);
            continue;
        }

        line = so->line;
        if (line == NULL) {               // EOF 센티넬
            not = 1;
            wait = 1;
            pthread_mutex_unlock(&so->lock);
            break;
        }

        // 여기서 line에는 최대 10줄이 붙어 있음.
        // 필요하면 줄 단위로 파싱해서 사용 가능.
        // 예) fwrite(line, 1, strlen(line), stdout);

        free(so->line);                   // 배치 전체 해제
        i++;
        so->full = 0;
        wait = 1;
        pthread_mutex_unlock(&so->lock);
    }

 //   printf("Cons: %d lines (batches)\n", i);
    write_line += i;
    *ret = i;
    pthread_exit(ret);
}

int main (int argc, char *argv[])
{
    clock_t c0 = clock();
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc; long t;
    int *ret;
    int i;
    FILE *rfile;

    if (argc == 1) {
  //      printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit(0);
    }

    so_t *share = malloc(sizeof(so_t));
    memset(share, 0, sizeof(so_t));
    rfile = fopen((char *) argv[1], "r");
    if (rfile == NULL) { perror("rfile"); exit(0); }

    Nprod = (argv[2] ? atoi(argv[2]) : 1);
    Ncons = (argv[3] ? atoi(argv[3]) : 1);
    if (Nprod > 100) Nprod = 100; if (Nprod == 0) Nprod = 1;
    if (Ncons > 100) Ncons = 100; if (Ncons == 0) Ncons = 1;

    share->rfile = rfile;
    share->line = NULL;
    pthread_mutex_init(&share->lock, NULL);

    for (i = 0; i < Nprod; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, share);

  //  printf("main continuing\n");

    for (i = 0; i < Ncons; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
    //    printf("main: consumer_%d joined with %d\n", i, *ret);
        free(ret);
    }
    for (i = 0; i < Nprod; i++) {
        rc = pthread_join(prod[i], (void **) &ret);
  //      printf("main: producer_%d joined with %d\n", i, *ret);
        free(ret);
    }

    printf("Cons(batches): %d\n", write_line);
    printf("Prod(lines):   %d\n", read_line);

    clock_t c1 = clock();
    printf("CPU time: %.3f ms\n", 1000.0 * (c1 - c0) / CLOCKS_PER_SEC);

    pthread_mutex_destroy(&share->lock);
    fclose(rfile);
    free(share);
    return 0;
}

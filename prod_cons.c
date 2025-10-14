
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#define BUF_SIZE 10000
#define BATCH_SIZE 100000


int stat[26];

pthread_mutex_t stat_mtx = PTHREAD_MUTEX_INITIALIZER;
// 로컬로 세고

static void count_alpha_local(const unsigned char *s, size_t n, int local[26]) {
    for (int i = 0; i < 26; ++i) local[i] = 0;
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = s[i];
        if (c >= 'A' && c <= 'Z') local[c - 'A']++;
        else if (c >= 'a' && c <= 'z') local[c - 'a']++;
    }
}

// 전역에 짧게 합치기
static void merge_counts(const int local[26]) {
    pthread_mutex_lock(&stat_mtx);
    for (int i = 0; i < 26; ++i) stat[i] += local[i];
    pthread_mutex_unlock(&stat_mtx);
}
void print_counts() {
    for (int i = 0; i < 26; ++i) {                    // 0..25
        if (stat[i] > 0)
            printf("%c: %d ", 'a' + i, stat[i]);     // stat[i] 출력
        if ((i+1) % 5 == 0) printf("\n");
    }
    printf("\n");
}


typedef struct sharedobject {
    FILE *rfile;

    // 여러 producer가 동시에 읽지 않도록 파일용 뮤텍스
    pthread_mutex_t file_mtx;
    // 원형 큐 버퍼
    char *line[BUF_SIZE];
    size_t  line_len[BUF_SIZE];   // 각 배치의 유효 길이
    int head, tail;
    int count;          // 현재 저장된 개수 (빈 큐: 0)
    int nprod;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;

    int end;              // EOF 도달(더 이상 push 없음)
} so_t;

void enque(so_t *so, char *batch, size_t blen){
    so->line[so->tail] = batch;
    so->line_len[so->tail] = blen;      // ★ 길이 저장
    so->tail = (so->tail + 1) % BUF_SIZE;
    so->count++;
}
char* deque(so_t *so, size_t *out_len){
    char* batch = so->line[so->head]; 
    if (out_len) *out_len = so->line_len[so->head];   // ★ 길이 꺼내기
    so->head = (so->head + 1) % BUF_SIZE;
    so->count--;
    return batch;
}

void *producer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);
    *ret = 0;

    char  *line = NULL;
    size_t len  = 0;
    ssize_t read;
    while(1){
        char *batch = NULL;  
        // bcap: batch 용량, blen: batch 바이트의 수
        size_t bcap = 0, blen = 0; 
        // got: 이번 배치에서 읽어 붙인 줄 개수
        int got = 0;
        pthread_mutex_lock(&so->file_mtx);
        for(int k = 0; k < BATCH_SIZE; k++){
            read = getdelim(&line, &len, '\n', so->rfile);
            if(read == -1) break;
            // 공간이 부족할 시 -> 새로할당(realloc)
            if (blen + (size_t)read + 1 > bcap) {
                size_t nb = (blen + (size_t)read + 1) * 2;
                char *tmp = realloc(batch, nb);
                if (!tmp) {
                    pthread_mutex_unlock(&so->file_mtx); // 빠져나가기 전 해제
                    free(batch);
                    free(line);
                    perror("realloc");
                    pthread_exit(NULL);
                }
                batch = tmp; bcap = nb;
            }
            // batch 뒤에 바로 복사 -> memcpy 안전
            memcpy(batch + blen, line, (size_t)read);
            blen += (size_t)read;
            batch[blen] = '\0';
            got++;
        }
        pthread_mutex_unlock(&so->file_mtx);
        if(got == 0){
            pthread_mutex_lock(&so->lock);
            so->nprod--;
            if(so->nprod == 0){
                so->end = 1;
                pthread_cond_broadcast(&so->not_empty);
            }
            pthread_mutex_unlock(&so->lock);
            free(batch);
            break;
            
        }

        // 큐에 push
        pthread_mutex_lock(&so->lock);
        while (so->count == BUF_SIZE) {
            pthread_cond_wait(&so->not_full, &so->lock);
        }
        // 기존
        // enque(so, batch);

        // 변경
        enque(so, batch, blen);
        pthread_cond_signal(&so->not_empty);
        pthread_mutex_unlock(&so->lock);

        (*ret)+=got;
    }

    free(line);
    printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), *ret);
    pthread_exit(ret);
}

void *consumer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);
    *ret = 0;
    int i = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->count == 0 && !so->end) {
            pthread_cond_wait(&so->not_empty, &so->lock);
        }
        if (so->count == 0 && so->end) {
            pthread_mutex_unlock(&so->lock);
            break; // 더 이상 데이터 없음
        }
        size_t blen = 0;
        char *batch = deque(so, &blen);


        //파일 작은면 출력해도 상관 없음
        //printf("%s",batch);

        pthread_cond_signal(&so->not_full);
        pthread_mutex_unlock(&so->lock);

        //int local[26];
        //count_alpha_local((const unsigned char*)batch, blen, local); // ★ 길이 기반
        //merge_counts(local);


        free(batch);
        i++;
    }

    printf("Cons_%x: %d lines\n", (unsigned int)pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}

int main (int argc, char *argv[])
{
    printf("Buffer_size = %d, Batch_size = %d\n", BUF_SIZE, BATCH_SIZE);
    clock_t c0 = clock();
    memset(stat, 0, sizeof(stat));

    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;
    int *ret;
    int i;
    FILE *rfile;

    if (argc < 2) {
        printf("usage: %s <readfile> [#Producer] [#Consumer]\n", argv[0]);
        return 0;
    }

    so_t *share = calloc(1, sizeof(so_t));
    if (!share) { perror("calloc"); return 1; }

    rfile = fopen((char *) argv[1], "rb");
    if (rfile == NULL) {
        perror("rfile");
        free(share);
        return 1;
    }

    Nprod = (argc >= 3) ? atoi(argv[2]) : 1;
    if (Nprod < 1) Nprod = 1; if (Nprod > 100) Nprod = 100;
    Ncons = (argc >= 4) ? atoi(argv[3]) : 1;
    if (Ncons < 1) Ncons = 1; if (Ncons > 100) Ncons = 100;

    share->rfile = rfile;
    share->count = 0;    
    share->end   = 0;
    share->head  =0;
    share->tail = 0;
    share->nprod = Nprod;

    pthread_mutex_init(&share->file_mtx, NULL);
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->not_empty, NULL);
    pthread_cond_init(&share->not_full, NULL);

    for (i = 0 ; i < Nprod ; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0 ; i < Ncons ; i++)
        pthread_create(&cons[i], NULL, consumer, share);

    int total_cons = 0, total_prod = 0;

    for (i = 0 ; i < Ncons ; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
        if (rc == 0 && ret) {
            total_cons += *ret;
            printf("main: consumer_%d joined with %d\n", i, *ret);
            free(ret);
        }
    }
    for (i = 0 ; i < Nprod ; i++) {
        rc = pthread_join(prod[i], (void **) &ret);
        if (rc == 0 && ret) {
            total_prod += *ret;
            printf("main: producer_%d joined with %d\n", i, *ret);
            free(ret);
        }
    }

    clock_t c1 = clock();
    printf("CPU time: %.3f ms\n", 1000.0 * (c1 - c0) / CLOCKS_PER_SEC);

    // 남은 버퍼 정리(정상 종료라면 없음)
    pthread_mutex_lock(&share->lock);
    while (share->count > 0) {
        free(share->line[share->head]);
        share->head = (share->head + 1) % BUF_SIZE;
        share->count--;
    }

    pthread_mutex_unlock(&share->lock);
    pthread_cond_destroy(&share->not_empty);
    pthread_cond_destroy(&share->not_full);
    pthread_mutex_destroy(&share->lock);
    pthread_mutex_destroy(&share->file_mtx);
    pthread_mutex_destroy(&stat_mtx);
    fclose(rfile);
    free(share);
    //print_counts();
    return 0;
}

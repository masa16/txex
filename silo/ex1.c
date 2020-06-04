/* gcc ex1.c -o ex1 -g -W -Wall -lpthread -std=gnu99 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <time.h>

#define DEBUG 0

#if DEBUG
#define NUM_THREADS 4
#define NUM_DATA 5
#define TX_LEN 5
#define N_REPEAT (12/NUM_THREADS)
#else
#define NUM_THREADS 4
#define NUM_DATA 10
#define TX_LEN 30
//#define N_REPEAT (400000/NUM_THREADS)
#define N_REPEAT (4000/NUM_THREADS)
#endif

typedef struct _DATA {
    int val;
    int tid;
    pthread_mutex_t lock;
    bool locked;
} DATA;

typedef enum {NONE=0, READ=1, WRITE=2} TYPE;

typedef struct _XACT {
    int key;
    TYPE type;
} XACT;

DATA Database[NUM_DATA];
pthread_t threads[NUM_THREADS];
XACT xact[TX_LEN*N_REPEAT][NUM_THREADS];

static struct timespec start_time;

void init_time()
{
    clock_gettime(CLOCK_MONOTONIC, &start_time);
}

int get_time()
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - start_time.tv_sec)*1000000 + t.tv_nsec/1000;
}

#define LOCK(k)                                 \
    {                                           \
        t = get_time();                         \
        pthread_mutex_lock(&Database[k].lock);  \
        Database[k].locked = true;              \
        tsum += get_time() - t;                 \
}
#define UNLOCK(k)                               \
    {                                           \
        Database[k].locked = false;             \
        pthread_mutex_unlock(&Database[k].lock);\
    }

void *worker(void *arg)
{
    TYPE type[NUM_DATA];
    int  val[NUM_DATA];
    int  tid[NUM_DATA];
    XACT *xact = (XACT*)arg;
    int t, tsum = 0;
    int t_begin = get_time(), t_end;
    double t_elap, t_lock;
    int n_abort=0;
    int n_commit=0;
    int commit_tid;

    for (int repeat=0; repeat < N_REPEAT; repeat++) {

        for (int k=0; k<NUM_DATA; k++) {
            type[k] = NONE;
            val[k] = 0;
            tid[k] = 0;
        }

        // get Read/Write set
        for (int i=0; i<TX_LEN; i++) {
            type[xact[i].key] |= xact[i].type;
        }

    retry:

        //printf("%d:phase1\n",repeat);
        // Phase 1 (lock)
        commit_tid = 0;
        for (int k=0; k<NUM_DATA; k++) {
            // lock write set
            if (type[k] & WRITE) {
                LOCK(k);
            }
            // read data
            if (type[k] & READ) {
                int t;
                val[k] = Database[k].val;
                tid[k] = t = Database[k].tid;
                if (t > commit_tid) commit_tid = t;
            }
        }

        // modify
        for (int i=0; i<TX_LEN; i++) {
            if (xact[i].type == READ) {
                val[xact[i].key] += 1;
            }
        }

        //printf("%d:phase2\n",repeat);
        // Phase 2 (validate)
        for (int k=0; k<NUM_DATA; k++) {
            if ( ((type[k]&READ) && tid[k]!=Database[k].tid) ||
                 ((type[k]==READ) && Database[k].locked) ) {
                // unlock write set
                for (int j=0; j<NUM_DATA; j++) {
                    if (type[j] & WRITE) {
                        UNLOCK(j);
                    }
                }
                n_abort += 1;
                goto retry;
            }
        }
        // commit tid
        for (int k=0; k<NUM_DATA; k++) {
            if (type[k] & WRITE) {
                int t = Database[k].tid;
                if (t > commit_tid) commit_tid = t;
            }
        }
        commit_tid++;

#if DEBUG
        for (int i=0; i<TX_LEN; i++) {
            printf(" %c%d",(xact[i].type==READ) ? 'r':'w', xact[i].key);
        }
        printf("\n");
#endif

        //printf("%d:phase3\n",repeat);
        // Phase 3 (write)
        for (int k=0; k<NUM_DATA; k++) {
            if (type[k] & WRITE) {
                Database[k].val = val[k];
                Database[k].tid = commit_tid;
                UNLOCK(k);
            }
#if DEBUG
            printf("value[%d]=%d Database[%d].val=%d tid=%d\n",
                   k,val[k],k,Database[k].val,tid[k]);
#endif
        }

        n_commit += 1;
        xact += TX_LEN;
    }
    t_end = get_time();
    t_elap = (t_end-t_begin)*1e-6;
    t_lock = tsum*1e-6;
    printf("time: elap=%f lock=%f lock_ratio=%f n_abort=%d n_commit=%d\n",
           t_elap,t_lock,t_lock/t_elap,n_abort,n_commit);

    return NULL;
}


int main(){
    int i, j, sum;

    // Initialize Database
    for (i=0; i<NUM_DATA; i++) {
        Database[i].val = 0;
        Database[i].tid = 0;
        Database[i].locked = false;
        pthread_mutex_init(&Database[i].lock, 0);
    }

    // Create Transaction
    sum = 0;
    for(i=0; i<NUM_THREADS; i++){
#if DEBUG
        printf("thread%d:",i);
#endif
        for(j=0; j<TX_LEN*N_REPEAT; j++){
            xact[j][i].type = (random()&1) ? READ : WRITE;
            xact[j][i].key = (int)(random()/(1.0+RAND_MAX) * NUM_DATA);
#if DEBUG
            printf(" %c%d",(xact[j][i].type==READ) ? 'r':'w', xact[j][i].key);
#endif
            if (xact[j][i].type==READ) {
                sum += 1;
            }
        }
#if DEBUG
        printf("\n");
#endif
    }
    printf("# of READ=%d\n",sum);
    init_time();

    // Start threads
    for(i=0; i<NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, worker, &xact[i]);
    }

    // Join threads
    for(i=0; i<NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Print result
    sum = 0;
    for (i=0; i<NUM_DATA; i++) {
        printf("%d ",Database[i].val);
        sum += Database[i].val;
    }
    printf("\nsum=%d\n",sum);

    return 0;
}

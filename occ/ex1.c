/* gcc ex1.c -o ex1 -g -W -Wall -lpthread -std=gnu99 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <time.h>

#define FORWARD_ALGORITHM 0
#define SECOND_ALGORITHM 0
#define DEBUG 0
#define ABORT_USLEEP 1

#if DEBUG
#define NUM_THREADS 4
#define NUM_DATA 5
#define TX_LEN 13
#define N_REPEAT (12/NUM_THREADS)
#else
#define NUM_THREADS 4
#define NUM_DATA 10
#define TX_LEN 30
#define N_REPEAT (400000/NUM_THREADS)
#endif

typedef struct _DATA {
    int val;
    //pthread_rwlock_t lock;
} DATA;

typedef enum {NONE=0, READ=1, WRITE=2} TYPE;

typedef struct _XACT {
    int key;
    TYPE type;
} XACT;

typedef struct _TX {
    int *values;
    TYPE *types;
} TX;

DATA Database[NUM_DATA];
pthread_t threads[NUM_THREADS];
XACT xact[TX_LEN*N_REPEAT][NUM_THREADS];
TX tx_seq[N_REPEAT*NUM_THREADS];
int tid_global=0;
pthread_mutex_t giant_lock;

#if FORWARD_ALGORITHM
TX *act_tx[NUM_THREADS];
int act_tx_len = 0;
#endif

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


void delete_from_set(TX **a, int *n, TX *item)
{
    int i=0;
    for (; i<*n; i++) {
        if (a[i]==item) {
            (*n)--;
            break;
        }
    }
    for (; i<*n; i++) {
        a[i] = a[i+1];
    }
}


#define LOCK()                              \
    {                                       \
        t = get_time();                     \
        pthread_mutex_lock(&giant_lock);    \
        tsum += get_time() - t;             \
    }
#define UNLOCK()                            \
    {                                       \
        pthread_mutex_unlock(&giant_lock);  \
    }

void *worker(void *arg)
{
    XACT *xact = (XACT*)arg;
    int tid_start, tid_end;
    int t, tsum = 0;
    int t_begin = get_time(), t_end;
    double t_elap, t_lock;
    TX tx;
    int n_abort=0;
    int n_commit=0;
#if FORWARD_ALGORITHM
        int n_act;
        TX *fin_act_tx[NUM_THREADS];
#endif

    for (int repeat=0; repeat < N_REPEAT; repeat++) {

        tx.types = malloc(sizeof(TYPE)*NUM_DATA);
        tx.values = malloc(sizeof(int)*NUM_DATA);
        for (int k=0; k<NUM_DATA; k++) {
            tx.types[k] = NONE;
        }

    retry:

        // Read phase
        tid_start = tid_global;
        for (int i=0; i<TX_LEN; i++) {
            int k = xact[i].key;
            tx.types[k] |= xact[i].type;
            if (xact[i].type == READ) {
                tx.values[k] = Database[k].val;
            }
        }

        // modify
        for (int i=0; i<TX_LEN; i++) {
            if (xact[i].type == READ) {
                tx.values[xact[i].key] += 1;
            }
        }

        // Validation
#if FORWARD_ALGORITHM
        LOCK();
        tid_end = tid_global;
        n_act = act_tx_len;
        if (act_tx_len>0)
            memcpy(fin_act_tx, act_tx, sizeof(TX*)*act_tx_len);
        act_tx[act_tx_len] = &tx;
        act_tx_len++;
        UNLOCK();

        for (int i = tid_start; i < tid_end; i++) {
            for (int k=0; k<NUM_DATA; k++) {
                // writeset of tid intersects my readset
                if (tx_seq[i].types[k] & WRITE && tx.types[k] & READ) {
                    // abort
                    LOCK();
                    delete_from_set(act_tx,&act_tx_len,&tx);
                    UNLOCK();
                    n_abort += 1;
                    usleep(ABORT_USLEEP);
                    goto retry;
                }
            }
        }
        for (int i = 0; i < n_act; i++) {
            for (int k=0; k<NUM_DATA; k++) {
                // writeset of tid intersects my readset
                if (fin_act_tx[i]->types[k] & WRITE && tx.types[k] & (READ|WRITE)) {
                    // abort
                    LOCK();
                    delete_from_set(act_tx,&act_tx_len,&tx);
                    UNLOCK();
                    n_abort += 1;
                    usleep(ABORT_USLEEP);
                    goto retry;
                }
            }
        }

#else // backward-oriented OCC

#if SECOND_ALGORITHM
        tid_end = tid_global;
        for (int i = tid_start; i < tid_end; i++) {
            for (int k=0; k<NUM_DATA; k++) {
                // writeset of tid intersects my readset
                if (tx_seq[i].types[k] & WRITE && tx.types[k] & READ) {
                    // abort
                    n_abort += 1;
                    usleep(ABORT_USLEEP);
                    goto retry;
                }
            }
        }
        tid_start = tid_end;
#endif
        LOCK();
        tid_end = tid_global;
        for (int i = tid_start; i < tid_end; i++) {
            for (int k=0; k<NUM_DATA; k++) {
                // writeset of tid intersects my readset
                if (tx_seq[i].types[k] & WRITE && tx.types[k] & READ) {
                    // abort
                    UNLOCK();
                    n_abort += 1;
                    usleep(ABORT_USLEEP);
                    goto retry;
                }
            }
        }

#endif // FORWARD_ALGORITHM

#if DEBUG
        for (int i=0; i<TX_LEN; i++) {
            printf(" %c%d",(xact[i].type==READ) ? 'r':'w', xact[i].key);
        }
        printf("\n");
#endif

        // Write phase
        for (int k=0; k<NUM_DATA; k++) {
            if (tx.types[k] & WRITE) {
                Database[k].val = tx.values[k];
            }
#if DEBUG
            printf("tx.values[%d]=%d Database[%d].val=%d\n",
                   k,tx.values[k],k,Database[k].val);
#endif
        }

#if FORWARD_ALGORITHM
        LOCK();
        delete_from_set(act_tx,&act_tx_len,&tx);
#endif
        tx_seq[tid_global] = tx;
        tid_global += 1;
        UNLOCK();
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
    pthread_mutex_init(&giant_lock, 0);

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

    for (i=0; i<tid_global; i++) {
        free(tx_seq[i].types);
        free(tx_seq[i].values);
    }

    return 0;
}

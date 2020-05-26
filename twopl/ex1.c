/* gcc ex1.c -o ex1 -g -W -Wall -lpthread -std=gnu99 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <time.h>

#define NUM_THREADS 4
#define NUM_DATA 10
#define TX_LEN 30
#define N_REPEAT (400000/NUM_THREADS)

typedef struct _DATA {
    int val;
    pthread_rwlock_t lock;
} DATA;

typedef enum {NONE=0, READ=1, WRITE=2} TYPE;

typedef struct _XACT {
    int key;
    TYPE type;
} XACT;

DATA Database[NUM_DATA];
pthread_t tid[NUM_THREADS];
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


void *worker(void *arg)
{
    TYPE types[NUM_DATA];
    int values[NUM_DATA];
    XACT *xact = (XACT*)arg;
    int t, tsum = 0;
    int t_begin = get_time(), t_end;
    double t_elap, t_lock;

    for (int repeat=0; repeat < N_REPEAT; repeat++) {

        for (int i=0; i<NUM_DATA; i++) {
            types[i] = NONE;
            values[i] = 0;
        }

        // Growing phase
        for (int i=0; i<TX_LEN; i++) {
            types[xact[i].key] |= xact[i].type;
        }
        for (int i=0; i<NUM_DATA; i++) {
            if (types[i] & WRITE) {
                t = get_time();
                pthread_rwlock_wrlock(&Database[i].lock);
                tsum += get_time() - t;
            }
            else if (types[i] == READ) {
                t = get_time();
                pthread_rwlock_rdlock(&Database[i].lock);
                tsum += get_time() - t;
            }
            if (types[i] & READ) {
                values[i] = Database[i].val;
            }
        }

        // modify
        for (int i=0; i<TX_LEN; i++) {
            if (xact[i].type == READ) {
                values[xact[i].key] += 1;
            }
        }

        // Shrinking phase
        for (int i=0; i<NUM_DATA; i++) {
            if (types[i] & WRITE) {
                Database[i].val = values[i];
            }
            if (types[i] & (READ|WRITE)) {
                pthread_rwlock_unlock(&Database[i].lock);
            }
            //printf("values[%d]=%d Database[%d].val=%d\n",i,values[i],i,Database[i].val);
        }

        xact += TX_LEN;
    }
    t_end = get_time();
    t_elap = (t_end-t_begin)*1e-6;
    t_lock = tsum*1e-6;
    printf("time: elap=%f lock=%f lock_ratio=%f\n",t_elap,t_lock,t_lock/t_elap);

    return NULL;
}


int main(){
    int i, j, sum;

    // Initialize Database
    for (i=0; i<NUM_DATA; i++) {
        Database[i].val = 0;
        pthread_rwlock_init(&Database[i].lock, 0 );
    }
    // Create Transaction
    sum = 0;
    for(i=0; i<NUM_THREADS; i++){
        //printf("thread%d:",i);
        for(j=0; j<TX_LEN*N_REPEAT; j++){
            xact[j][i].type = (random()&1) ? READ : WRITE;
            xact[j][i].key = (int)(random()/(1.0+RAND_MAX) * NUM_DATA);
            //printf(" %c%d",(xact[j][i].type==READ) ? 'r':'w', xact[j][i].key);
            if (xact[j][i].type==READ) {
                sum += 1;
            }
        }
        //printf("\n");
    }
    printf("# of READ=%d\n",sum);
    init_time();
    // Start threads
    for(i=0; i<NUM_THREADS; i++) {
        pthread_create(&tid[i], NULL, worker, &xact[i]);
    }
    // Join threads
    for(i=0; i<NUM_THREADS; i++) {
        pthread_join(tid[i], NULL);
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

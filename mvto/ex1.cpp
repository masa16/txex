/* g++ ex1.cpp -o ex1 -g -W -Wall -lpthread  */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <time.h>

#include <atomic>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <random>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <list>
#include <unordered_map>
#include <functional>

#define DEBUG 0

#if DEBUG
#define N_TRANSACTION 1200
#define NUM_THREADS 4
#define NUM_DATA 10
#define TX_LEN 5
#else
//#define N_REPEAT (400000/NUM_THREADS)
//#define N_REPEAT (12000/NUM_THREADS)
#define N_TRANSACTION 400000
#define NUM_THREADS 4
#define NUM_DATA 50
#define TX_LEN 10
#endif
#define N_REPEAT (N_TRANSACTION/NUM_THREADS)

typedef int Value;

typedef struct _DATA {
    int val;
} DATA;

typedef enum {NONE=0, READ=1, WRITE=2} TYPE;

typedef struct _XACT {
    int key;
    TYPE type;
} XACT;

typedef struct _VersionValue {
    int version;
    Value value;
} VersionValue;

typedef struct _ReadRange {
    int rd_ts;
    int tx_ts;
} ReadRange;

typedef struct _ThreadResult {
    double t_elap;
    int n_abort;
} ThreadResult;


class DataItem {
    std::list<VersionValue> list;
    std::list<ReadRange> read_range;
    std::shared_mutex mtx;
public:
    DataItem() : DataItem(0) {}
    DataItem(Value value) : list(1,{0,value}), read_range(0) {}

    Value read(int timestamp) {
        Value value = list.begin()->value; // read first_item == max_version
        int ver = list.begin()->version;
        std::shared_lock<std::shared_mutex> lock(mtx);
        if (timestamp >= ver) {
            if (timestamp > ver + 1) {
#if DEBUG
                printf("r%d(x%d)",timestamp,ver);
#endif
                for (auto itr = read_range.begin(); ; itr++) {
                    if (itr == read_range.end() || itr->rd_ts < ver) {
                        read_range.insert(itr, {ver,timestamp});
                        break;
                    } else
                    if (itr->rd_ts == ver) {
                        if (itr->tx_ts < timestamp) {
                            itr->tx_ts = timestamp;
                        }
                        break;
                    }
                }
            }
        } else {
            // find max_version with <= timestamp
            for (auto itr = list.begin(); itr != list.end(); ++itr) {
                if (itr->version <= timestamp) {
                    value = itr->value;
                    break;
                }
            }
        }
        return value;
    }

    bool write(int timestamp, Value value) {
        // ri[xj], j=rd_ts < timestamp < i=tx_ts
        std::shared_lock<std::shared_mutex> lock(mtx);
        for (auto itr = read_range.begin(); itr != read_range.end(); ++itr) {
            if (itr->rd_ts < timestamp && timestamp < itr->tx_ts) {
#if DEBUG
                printf("(abort %d<%d<%d)\n", itr->rd_ts, timestamp, itr->tx_ts);
#endif
                return false;
            }
        }
        for (auto itr = list.begin(); itr != list.end(); itr++) {
            //printf("[%d,%d,%d(%d)]\n",timestamp,i1->version,itr->version,value);
            if (itr->version == timestamp) {
                itr->value = value;
                //printf("w%d(%d)",timestamp,value);
                break;
            } else
                if (itr->version < timestamp) {
                    VersionValue vv = {timestamp,value};
                    //printf("W%d,%d,%d(%d)",timestamp,i1->version,itr->version,value);
                    list.insert(itr, vv);
                    break;
                }
        }
        return true;
    }

    void gc(int timestamp) {
        std::lock_guard<std::shared_mutex> lock(mtx);
        if (!list.empty()) {
            for (auto itr=list.begin(); itr->version!=0;) {
                auto itr2=itr;
                itr2++;
                if (itr->version < timestamp) {
                    list.erase(itr);
                }
                itr=itr2;
            }
        }
        while (!read_range.empty()) {
            int v = read_range.back().rd_ts;
            if (v >= timestamp) break;
            read_range.pop_back();
        }
    }

    void print() {
        printf("ver:val(n=%ld){",std::distance(list.begin(),list.end()));
        for (auto itr = list.begin(); itr != list.end(); itr++) {
            printf("%d:%d,",itr->version,itr->value);
        }
        printf("}\n");
        printf("ri[xj](n=%ld){",std::distance(read_range.begin(),read_range.end()));
        for (auto itr = read_range.begin(); itr != read_range.end(); itr++) {
            printf("%d:%d,",itr->tx_ts,itr->rd_ts);
        }
        printf("}\n");
    }
};


class TimeStampGenerator {
    std::atomic<int> global_ts=1;
    std::list<int> ts_list;
    std::mutex mtx;
    std::atomic<int> gc_count_=0;
public:

    int gc_count() {
        return gc_count_.load();
    }

    int get_timestamp() {
        int ts = global_ts.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(mtx);
            ts_list.push_back(ts);
        }
        return ts;
    }

    void transaction_end(int timestamp, DataItem *database) {
        int ts0, ts1=0;
        {
            std::lock_guard<std::mutex> lock(mtx);
            ts0 = ts_list.front();
            if (ts0 == timestamp) {
                ts_list.pop_front();
                if (!ts_list.empty()) {
                    ts1 = ts_list.front();
                }
            } else {
                for (auto itr=ts_list.begin(); itr!=ts_list.end(); itr++) {
                    if (*itr == timestamp) {
                        ts_list.erase(itr);
                        return;
                    }
                }
            }
        }
        if (ts1>0 && ts1-ts0 > 10) {
            gc_count_.fetch_add(1);
#if DEBUG
            printf("\nGC: ts0=%d ts1=%d ts_list.size=%ld\n",ts0,ts1,ts_list.size());
#endif
            for (int i=0; i<NUM_DATA; i++) {
                database[i].gc(ts1);
            }
        }
    }
};


class Timer {
    struct timespec start_time;
public:
    Timer() {clock_gettime(CLOCK_MONOTONIC, &start_time);}

    double get_time()
    {
        struct timespec t;
        clock_gettime(CLOCK_MONOTONIC, &t);
        return t.tv_sec - start_time.tv_sec + (t.tv_nsec - start_time.tv_nsec)/1e9;
    }
};


void worker(int thread_id, std::vector<std::vector<XACT>> *xact_vec,
            DataItem *database, TimeStampGenerator *tsg, ThreadResult *result)
{
    Timer timer;
    double t_elap;
    int n_abort=0;

    for (int repeat=0; repeat < N_REPEAT; repeat++) {
        std::vector<XACT> xact = (*xact_vec)[repeat];
    retry:
        int ts = tsg->get_timestamp();
        std::unordered_map<int,Value> values;

        // Read phase
        for (int i=0; i<TX_LEN; i++) {
            int key = xact[i].key;
            int type = xact[i].type;
            Value v;
            if (type == READ) {
                values[key] = v = database[key].read(ts) + 1;
            }
            if (type == WRITE) {
                auto itr = values.find(key);
                v = (itr == values.end()) ? 0 : values[key];
                bool success = database[key].write(ts, v);
                if (!success) {
                    n_abort++;
                    tsg->transaction_end(ts,database);
                    usleep(1);
                    goto retry;
                }
            }
        }
        tsg->transaction_end(ts,database);
    }
    result->t_elap = t_elap = timer.get_time();
    result->n_abort = n_abort;
    //t_lock = tsum*1e-6;
    printf("thread%d: throughput=%f[tpx] time=%f[s] n_abort=%d abort_ratio=%f\n",
           thread_id,N_REPEAT/t_elap,t_elap,n_abort,n_abort*1.0/N_REPEAT);
}


int main(){
    int sum;
    std::vector<std::vector<std::vector<XACT>>>
        xact(NUM_THREADS,std::vector<std::vector<XACT>>(N_REPEAT,std::vector<XACT>(TX_LEN)));
    std::mt19937 mt;
    std::uniform_int_distribution<> rand_type(0,1);
    std::uniform_int_distribution<> rand_key(0,NUM_DATA-1);

    // Create Transaction
    sum = 0;
    for(int i=0; i<NUM_THREADS; i++){
#if DEBUG
        std::cout << "thread" << i << ":";
#endif
        for(int j=0; j<N_REPEAT; j++){
            for (int k=0; k<TX_LEN; k++) {
                XACT &x = xact[i][j][k];
                x.type = rand_type(mt) ? READ : WRITE;
                x.key = rand_key(mt);
#if DEBUG
                std::cout << ((xact[i][j][k].type==READ) ? "r":"w") << xact[i][j][k].key;
#endif
                if (xact[i][j][k].type==READ) {
                    sum += 1;
                }
            }
#if DEBUG
            std::cout << " ";
#endif
        }
#if DEBUG
        std::cout << std::endl;
#endif
    }

    //std::vector<DataItem> database(NUM_DATA,0);
    DataItem database[NUM_DATA];
    TimeStampGenerator tsg;

    std::vector<std::thread> thv;
    ThreadResult result[NUM_THREADS];
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        thv.emplace_back(worker, i, &xact[i], database, &tsg, &result[i]);
    }

    for (auto& th : thv) th.join();

    std::cout << std::endl;
    for (int i=0; i<NUM_DATA; i++) {
        printf("data:%d\n",i);
        database[i].print();
    }
    int n_abort=0;
    double t_elap=0;
    for (size_t i = 0; i < NUM_THREADS; ++i) {
        t_elap += result[i].t_elap;
        n_abort += result[i].n_abort;
    }
    printf("throughput=%f[tpx] total_time=%f n_abort=%d abort_ratio=%f gc_count=%d\n",
           N_TRANSACTION/t_elap*NUM_THREADS,t_elap,n_abort,1.0*n_abort/N_TRANSACTION,tsg.gc_count());

    return 0;
}

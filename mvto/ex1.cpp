/* g++ ex1.cpp -o ex1 -g -std=c++17 -W -Wall -lpthread  */

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <list>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#define DEBUG 0

#if DEBUG
#define N_TRANSACTION 1200
#define NUM_THREADS 4
#define NUM_DATA 10
#define TX_LEN 5
#define DPRINTF(...) std::printf(__VA_ARGS__)
#else
#define N_TRANSACTION 280000
#define NUM_THREADS 28
#define NUM_DATA 50
#define TX_LEN 10
#define DPRINTF(...) {}
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
    std::atomic<struct _VersionValue*> next;
} VersionValue;

typedef struct _ReadRange {
    int wr_ver;
    int rd_ts;
    std::atomic<struct _ReadRange*> next;
} ReadRange;

typedef struct _ThreadResult {
    double t_elap;
    int n_abort;
} ThreadResult;


class DataItem {
    VersionValue list_end = {0,0,NULL};
    VersionValue list_begin = {0,0,&list_end};
    ReadRange range_end = {0,0,NULL};
    ReadRange range_begin = {0,0,&range_end};
    std::shared_mutex mtx;
public:
    //DataItem() : DataItem(0) {}
    //DataItem(Value value) : list(0), read_range(0), v0(value) {}

    Value read(int timestamp) {
        // timestamp=7 のトランザクションが x を読むとする。
        std::shared_lock<std::shared_mutex> lock(mtx);
        VersionValue *x = list_begin.next.load();
        // read first_item == max_version
        // listの先頭が最大バージョンである。
        Value value = x->value;
        int ver = x->version;
        // timestamp=7 より新しいバージョン x9 が書かれている場合
        // 7より小さい最大のバージョン x3 を見つけて読む。
        for ( ; x != NULL; x = x->next.load()) {
            if (x->version <= timestamp) {
                value = x->value;
                ver = x->version;
                break;
            }
        }
        // record ri[xj], i=rd_ts, j=wr_ver
        // x3 を読むとき、r7[x3] を ReadRange に加える。
        DPRINTF("r%d(x%d)",timestamp,ver);
        for (auto i = &range_begin; ; i=i->next.load()) {
        retry:
            ReadRange *x = i->next.load();
            if (x->next.load() == NULL || x->wr_ver < ver) {
                ReadRange *m = (ReadRange *)malloc(sizeof(ReadRange));
                m->wr_ver = ver;
                m->rd_ts = timestamp;
                m->next.store(x);
                bool b = i->next.compare_exchange_weak(x,m);
                if (!b) {
                    free(m);
                    goto retry;
                }
                break;
            } else
            if (x->wr_ver == ver) {
                // r5[x3] がある場合、r7[x3] に広げる
                if (x->rd_ts < timestamp) {
                    x->rd_ts = timestamp;
                }
                break;
            }
        }
        return value;
    }

    bool write(int timestamp, Value value) {
        std::shared_lock<std::shared_mutex> lock(mtx);
        // If a step of the form rj(xk) such that ts(tk) < ts(ti) < ts(tj)
        // has already been scheduled, then wi(x) is rejected and ti is aborted.
        for (auto itr = range_begin.next.load(); itr != NULL; itr=itr->next.load()) {
            // ri[xj], j=wr_ver < timestamp < i=rd_ts
            if (itr->wr_ver < timestamp && timestamp < itr->rd_ts) {
                DPRINTF("(abort %d<%d<%d)\n", itr->wr_ver, timestamp, itr->rd_ts);
                return false;
            }
        }
        // abortしなければ VersionValue のリストに加える
        for (auto itr = &list_begin;; ) {
            retry:
            VersionValue *x = itr->next.load();
            if (x->version < timestamp) {
                VersionValue *m = (VersionValue *)malloc(sizeof(VersionValue));
                m->version = timestamp;
                m->value = value;
                m->next.store(x);
                bool b = itr->next.compare_exchange_weak(x,m);
                if (!b) {
                    free(m);
                    goto retry;
                }
                break;
            } else
            if (x->version == timestamp) {
                x->value = value;
                return true;
            }
            itr = x;
        }
        return true;
    }

    void gc(int timestamp) {
        // timestamp より小さい item を削除する。
        std::lock_guard<std::shared_mutex> lock(mtx);
        for (auto itr = list_begin.next.load();; ) {
            VersionValue *x = itr->next.load();
            if (x == NULL) break;
            if (x->version < timestamp) {
                bool b = itr->next.compare_exchange_weak(x,&list_end);
                if (!b) return;
                for (;;) {
                    auto y = x->next.load();
                    if (y == NULL) break;
                    //printf("[0x%lx,%d,%d]",(long)x,x->version,x->value);fflush(stdout);
                    free(x);
                    x = y;
                }
            }
            itr = x;
        }
        for (auto itr = range_begin.next.load();; ) {
            ReadRange *x = itr->next.load();
            if (x == NULL) break;
            if (x->wr_ver < timestamp) {
                bool b=itr->next.compare_exchange_weak(x,&range_end);
                if (!b) return;
                for (;;) {
                    auto y = x->next.load();
                    if (y == NULL) break;
                    //printf(" 0x%lx ",(long)x);;fflush(stdout);
                    free(x);
                    x = y;
                }
            }
            itr = x;
        }
    }

    void print() {
        printf("ver:val(n=%ld){",0L);
        for (auto x = list_begin.next.load(); x != NULL; x = x->next.load()) {
            printf("%d:%d,",x->version,x->value);
        }
        printf("}\n");
        printf("ri[xj](n=%ld){", 0L);
        for (auto x = range_begin.next.load(); x != NULL; x = x->next.load()) {
            printf("%d:%d,",x->rd_ts,x->wr_ver);
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
        /*
          ts_list = [10,11,12,13,14] => 10未満でGCできる
          ↓ ts = 11,12 が終了         => ts != ts_list.front()
          ts_list = [10, 13,14]      => 10が残っているのでGCできない
          ↓ ts = 10 が終了            => ts == ts_list.front()
          ts_list = [13,14]          => 13の前までGCできる
         */
        if (ts1>0 && ts1-ts0 > 10) {
            gc_count_.fetch_add(1);
            DPRINTF("\nGC: ts0=%d ts1=%d ts_list.size=%ld\n",ts0,ts1,ts_list.size());
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
                    std::this_thread::sleep_for(std::chrono::nanoseconds(1));
                    goto retry;
                }
            }
        }
        tsg->transaction_end(ts,database);
    }
    result->t_elap = t_elap = timer.get_time();
    result->n_abort = n_abort;
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
        DPRINTF("thread%d:",i);
        for(int j=0; j<N_REPEAT; j++){
            for (int k=0; k<TX_LEN; k++) {
                XACT &x = xact[i][j][k];
                x.type = rand_type(mt) ? READ : WRITE;
                x.key = rand_key(mt);
                DPRINTF("%s%d",(xact[i][j][k].type==READ) ? "r":"w",xact[i][j][k].key);
                if (xact[i][j][k].type==READ) {
                    sum += 1;
                }
            }
            DPRINTF(" ");
        }
        DPRINTF("\n");
    }

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

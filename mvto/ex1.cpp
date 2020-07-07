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
} VersionValue;

typedef struct _ReadRange {
    int wr_ver;
    int rd_ts;
} ReadRange;

typedef struct _ThreadResult {
    double t_elap;
    int n_abort;
} ThreadResult;


class DataItem {
    std::list<VersionValue> list;
    std::list<ReadRange> read_range;
    Value v0;
    std::shared_mutex mtx;
public:
    DataItem() : DataItem(0) {}
    DataItem(Value value) : list(0), read_range(0), v0(value) {}

    Value read(int timestamp) {
        Value value;
        int ver;
        // read first_item == max_version
        if (list.empty()) {
            ver = 0;
            value = v0;
        } else {
            value = list.begin()->value;
            ver = list.begin()->version;
        }
        std::shared_lock<std::shared_mutex> lock(mtx);
        if (timestamp >= ver) {
            if (timestamp > ver + 1) {
                // record ri[xj], i=rd_ts, j=wr_ver
                DPRINTF("r%d(x%d)",timestamp,ver);
                for (auto itr = read_range.begin(); ; itr++) {
                    if (itr == read_range.end() || itr->wr_ver < ver) {
                        read_range.insert(itr, {ver,timestamp}); // must be atomic
                        break;
                    } else
                    if (itr->wr_ver == ver) {
                        if (itr->rd_ts < timestamp) {
                            itr->rd_ts = timestamp;
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
        // ri[xj], j=wr_ver < timestamp < i=rd_ts
        std::shared_lock<std::shared_mutex> lock(mtx);
        for (auto itr = read_range.begin(); itr != read_range.end(); ++itr) {
            if (itr->wr_ver < timestamp && timestamp < itr->rd_ts) {
                DPRINTF("(abort %d<%d<%d)\n", itr->wr_ver, timestamp, itr->rd_ts);
                return false;
            }
        }
        for (auto itr = list.begin(); itr != list.end(); itr++) {
            if (itr->version == timestamp) {
                itr->value = value;
                return true;
            } else
            if (itr->version < timestamp) {
                VersionValue vv = {timestamp,value};
                list.insert(itr, vv); // must be atomic
                return true;
            }
        }
        VersionValue vv = {timestamp,value};
        list.emplace_back(vv); // must be atomic
        return true;
    }

    void gc(int timestamp) {
        std::lock_guard<std::shared_mutex> lock(mtx);
        while (!list.empty()) {
            int v = list.back().version;
            if (v >= timestamp) break;
            list.pop_back();
        }
        while (!read_range.empty()) {
            int v = read_range.back().wr_ver;
            if (v >= timestamp) break;
            read_range.pop_back();
        }
    }

    void print() {
        printf("ver:val(n=%ld){",std::distance(list.begin(),list.end()));
        for (auto itr = list.begin(); itr != list.end(); itr++) {
            printf("%d:%d,",itr->version,itr->value);
        }
        printf("%d:%d",0,v0);
        printf("}\n");
        printf("ri[xj](n=%ld){",std::distance(read_range.begin(),read_range.end()));
        for (auto itr = read_range.begin(); itr != read_range.end(); itr++) {
            printf("%d:%d,",itr->rd_ts,itr->wr_ver);
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

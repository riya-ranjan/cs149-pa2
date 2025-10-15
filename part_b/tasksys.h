#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <unordered_set>
#include <vector>
#include <queue>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <cstdio>
#include <atomic>

struct TaskStruct {
    TaskID task_id;
    IRunnable* runnable_ptr;
    int total_tasks;
    std::unordered_set<TaskID> parent_tasks;
    int pending_task_id;
    std::atomic<int> mini_tasks_left;

    void remove_parent(TaskID finished_parent) {
        parent_tasks.erase(finished_parent);
    };
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void update_parent_tasks(std::unordered_set<TaskID> &deps);
        void update_finished_tasks(TaskStruct* finished_task);
        void thread_func();
    private:
        TaskID new_task_id;
        std::mutex queue_mutex;
        std::mutex finished_set_mutex;

        std::condition_variable cv_finished;
        std::condition_variable cv_wrkr;

        std::unordered_set<TaskStruct*> finished_tasks;
        std::queue<TaskStruct*> ready_tasks;
        std::vector<TaskStruct*> waiting_tasks;

        std::vector<std::thread> threads;

        bool stop{false};
};

#endif

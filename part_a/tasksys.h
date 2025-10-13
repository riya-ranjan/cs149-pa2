#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <cstdio>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>
#include <functional>
#include <condition_variable>

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
    private:
        int num_threads_;
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
        void thread_func();
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    
    private:
        std::vector<std::thread>          threads_;       // worker threads
        std::queue<std::function<void()>> tasks_;         // queue of tasks
        std::mutex                        queue_mutex_;   // lock for moving tasks of the queue 
        bool stop_{false};                                // once we have no tasks left, stop 
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
        void thread_func();
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        std::vector<std::thread>          threads_;       // worker threads
        std::queue<std::function<void()>> tasks_;         // queue of tasks
        std::mutex                        queue_mutex_;   // lock for moving tasks off the queue 
        std::mutex                        done_mutex_;    // lock for determining when to stop 
        bool stop_{false};                                // once we have no tasks left, stop
        std::condition_variable           cv_wrkr_;       // cv for worker threads
        std::condition_variable           cv_main_;       // cv for main thread     
        std::atomic<int>                  cur_task_id_;   // id of current task
        IRunnable*                        runnable_ptr_;   // ptr for runnable       
        int                               total_tasks_;    // total tasks to do
        std::atomic<int>                  tasks_left_;
};

#endif

#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // task counter
    std::atomic<int> task_id(0);

    // just do the next task
    auto thread_func = [&]() {
        while (true) {
            int next_task_id = task_id.fetch_add(1);
            if (next_task_id >= num_total_tasks) break;
            runnable->runTask(next_task_id, num_total_tasks);
        }
    };

    // create threads in place, thread_func is the function to run 
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads_; i++) {
        threads.emplace_back(thread_func);
    }

    // join at the end
    for (int i = 0; i < num_threads_; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    for (int i = 0; i < num_threads; i++) {
        threads_.emplace_back(
            &TaskSystemParallelThreadPoolSpinning::thread_func, this);
    }
}

void TaskSystemParallelThreadPoolSpinning::thread_func() {
    while (true) {
        if (stop_) return;

        std::function<void()> task;
    
        // if there are still tasks, enqueue them
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (!tasks_.empty()) {
                task = move(tasks_.front());
                tasks_.pop();
            } 
        }        

        // run the task if there is one
        if (task) {
            task();
        } else {
            std::this_thread::yield();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    for(auto &t : threads_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    std::atomic<int> tasks_left(num_total_tasks);

    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        for (int i = 0; i < num_total_tasks; i++) {
            int task_id = i; 
            tasks_.emplace([this, runnable, task_id, num_total_tasks, &tasks_left] {
                runnable->runTask(task_id, num_total_tasks);
                tasks_left.fetch_sub(1);
            });
        }
    }

    while(tasks_left.load() > 0) {
        std::this_thread::yield();
    } 
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    runnable_ptr_ = nullptr;
    cur_task_id_ = -1;
    total_tasks_ = -1;
    
    for (int i = 0; i < num_threads; i++) {
        threads_.emplace_back(
            &TaskSystemParallelThreadPoolSleeping::thread_func, this);
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_func() {
    int running_task = -1;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // reacquire lock if we have work to do or need to stop
            cv_wrkr_.wait(lock, [this] { return cur_task_id_ < total_tasks_ || stop_; });

            if (stop_ && cur_task_id_ == total_tasks_) return;

            running_task = cur_task_id_;
            cur_task_id_++;

        }
        runnable_ptr_->runTask(running_task, total_tasks_);
        if (tasks_left_.fetch_sub(1) == 1) {
            {
                std::unique_lock<std::mutex> lock(done_mutex_);
            }
            cv_main_.notify_one();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    cv_wrkr_.notify_all();
    for(auto &t : threads_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        runnable_ptr_ = runnable;
        cur_task_id_ = 0;
        total_tasks_ = num_total_tasks;
        tasks_left_ = num_total_tasks;
    }
    cv_wrkr_.notify_all();

    {
        std::unique_lock<std::mutex> lock(done_mutex_);
        cv_main_.wait(lock, [this] { return tasks_left_ == 0; });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    return;
}

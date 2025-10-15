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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
}
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    new_task_id = -1;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(
            &TaskSystemParallelThreadPoolSleeping::thread_func, this);
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_func() {
    int running_task = -1;
    TaskStruct* next_task = nullptr;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);

            cv_wrkr.wait(lock, [this] { return !ready_tasks.empty() || stop; });

            if (stop && ready_tasks.empty()) return; 

            next_task = ready_tasks.front();
            running_task = next_task->pending_task_id;
            next_task->pending_task_id++;
            if (running_task == next_task->total_tasks - 1) {
                ready_tasks.pop();
            }
        }
        if (next_task) {
            next_task->runnable_ptr->runTask(running_task, next_task->total_tasks);

            // if we're done with the last task, mark the overall task as unfinished
            if (next_task->mini_tasks_left.fetch_sub(1) == 1) {
                update_finished_tasks(next_task);
            }
        }
        next_task = nullptr;
    }
}

void TaskSystemParallelThreadPoolSleeping::update_finished_tasks(TaskStruct* finished_task) {
    bool has_tasks = false;
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        TaskID done_id = finished_task->task_id;
        finished_tasks.insert(finished_task);

        // update our ready tasks 
        if (!(ready_tasks.empty() && waiting_tasks.empty())) {
            has_tasks = true;
            for (auto it = waiting_tasks.begin(); it != waiting_tasks.end(); ) {
                (*it)->remove_parent(done_id);
                if (((*it)->parent_tasks).empty()) {
                    ready_tasks.emplace(*it);
                    it = waiting_tasks.erase(it);
                } else {
                    ++it;
                }
            }
        }
        

    }
    if (has_tasks) {
        cv_wrkr.notify_all();
    } else {
        cv_finished.notify_one();
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    cv_wrkr.notify_all();
    for (auto &t : threads) {
        t.join();
    }

    for (TaskStruct* t : finished_tasks) {
        delete t;
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps = {};
    TaskID this_task = runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    (void)this_task;
    sync();
}

void TaskSystemParallelThreadPoolSleeping::update_parent_tasks(std::unordered_set<TaskID> &deps) {
    for (TaskStruct* t : finished_tasks) {
        deps.erase(t->task_id);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    new_task_id++;
    std::unordered_set<TaskID> deps_set(deps.begin(), deps.end());
    bool added_to_ready_queue = false;

    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        update_parent_tasks(deps_set);


        TaskStruct* new_task = new TaskStruct{new_task_id,
                                              runnable,
                                              num_total_tasks,
                                              deps_set,
                                              0,
                                              {}};
        new_task->mini_tasks_left.store(num_total_tasks);

        if (deps_set.empty()) {
            ready_tasks.emplace(new_task);
            added_to_ready_queue = true;
        } else {
            waiting_tasks.emplace_back(new_task); 
        }
    }
    if (added_to_ready_queue) cv_wrkr.notify_all();
    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        cv_finished.wait(lock, [this] { return ready_tasks.empty() && waiting_tasks.empty(); });
    }
}

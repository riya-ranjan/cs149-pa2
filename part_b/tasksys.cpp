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
    bool done_with_tasks = false;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);

            cv_wrkr.wait(lock, [this] { return !ready_tasks.empty() || stop; });

            if (stop && ready_tasks.empty()) return; 

            next_task = ready_tasks.front();
            if (next_task->pending_task_id == next_task->total_tasks) {
                printf("The task %d is done after %d mini tasks have been finished\n", next_task->task_id, next_task->pending_task_id);
                ready_tasks.pop();
                done_with_tasks = !update_finished_tasks(next_task);
            } else {
                printf("Currently running mini task %d of task %d\n", next_task->pending_task_id, next_task->task_id);
                running_task = next_task->pending_task_id;
                next_task->pending_task_id++;
            }
        }
        if (next_task) {
            next_task->runnable_ptr->runTask(running_task, next_task->total_tasks);
        } else if (done_with_tasks) {
            cv_finished.notify_one();
        } else {
            cv_wrkr.notify_all();
        }
        next_task = nullptr;
    }
}

bool TaskSystemParallelThreadPoolSleeping::update_finished_tasks(TaskStruct* finished_task) {
    TaskID done_id = finished_task->task_id;
    delete finished_task;
    {
        std::unique_lock<std::mutex> lock(finished_set_mutex);
        printf("Adding task %d to finished tasks\n", done_id);
        finished_tasks.insert(done_id);
    }
    // free heap allocated memory
    // delete finished_task;

    // update our ready tasks
    bool has_tasks = false;
    {
        // std::unique_lock<std::mutex> lock(queue_mutex);

        // if we have no outstanding tasks, notify our main thread
        if (!(ready_tasks.empty() && waiting_tasks.empty())) {
            has_tasks = true;
            for (auto it = waiting_tasks.begin(); it != waiting_tasks.end(); ) {
                (*it)->remove_parent(done_id);
                if (((*it)->parent_tasks).empty()) {
                    printf("Moving task %d from waiting to ready\n", (*it)->task_id);
                    ready_tasks.emplace(*it);
                    it = waiting_tasks.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            printf("No more tasks left\n");
        }
    }
    return has_tasks;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // ImplementNotifations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::update_parent_tasks(std::unordered_set<TaskID> &deps) {
    printf("Checking dependencies...\n");
    {
        std::unique_lock<std::mutex> lock(finished_set_mutex);
        for (TaskID t : finished_tasks) {
            deps.erase(t);
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    new_task_id++;
    std::unordered_set<TaskID> deps_set(deps.begin(), deps.end());
    printf("Need to check dependencies for task %d\n", new_task_id);
    update_parent_tasks(deps_set);


    TaskStruct* new_task = new TaskStruct{new_task_id,
                                          runnable,
                                          num_total_tasks,
                                          deps_set,
                                          0};

    if (deps_set.empty()) {
        {
            printf("Task %d is ready to run\n", new_task_id);
            std::unique_lock<std::mutex> lock(queue_mutex);
            ready_tasks.emplace(new_task);
            printf("The size of the ready queue is %ld\n", ready_tasks.size());
        }
        cv_wrkr.notify_all();
    } else {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            printf("Task %d is not ready to run, adding to waiting queue\n", new_task_id);
            waiting_tasks.emplace_back(new_task);
        }
    }
    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        cv_finished.wait(lock, [this] { return ready_tasks.empty() && waiting_tasks.empty(); });
        printf("Setting stop to true...\n");
        stop = true;
    }
    cv_wrkr.notify_all();
    for (auto &t : threads) {
        t.join();
    }
}

/*
 * Comper.h
 *
 *  Created on: 2019骞�11鏈�25鏃�
 *      Author: wen
 */

#ifndef SYSTEM_COMPER_H_
#define SYSTEM_COMPER_H_

#include <mutex>
#include <thread>
#include <stack>
#include <queue>
#include <vector>
#include <condition_variable>

#include "Global.h"

using namespace std;

template<class TaskT>
class Comper{
public:

	//local task queue (type to be defined)
	void* ltask_queue;
	mutex lq_mtx;
	//local flag;
	bool local_end_label;
	size_t local_num_idle;
	ofstream* fouts; // output files for local thread; fouts[0] also used for current comper
	thread main_thread;


    virtual ~Comper()
    {
    	delete (stack<TaskT*> *)ltask_queue;
    	main_thread.join();
    	for(int i=0; i<THREADS; i++)
    		fouts[i].close();
    	delete[] fouts;
    }


	void init(string outfolder){
		ltask_queue = new stack<TaskT*>;
		local_end_label = false;
		local_num_idle = 0;

		//set output folder and output streams
		fouts = new ofstream[THREADS];
		for(int i=0; i<THREADS; i++)
			fouts[i].open(outfolder+to_string(i));
	}

	tqueue<TaskT>& gqueue(){ // get the global transaction database
		return *(tqueue<TaskT> *)task_queue;
	}
	stack<TaskT*>& lqueue(){ // get the global transaction database
		return *(stack<TaskT*> *)ltask_queue;
	}
	// ============== main logic begin =================
	//set of variables for conditioning on whether to run a task-processing thread
	mutex mtx_go;
	condition_variable cv_go;
	bool ready_go = true; //protected by mtx_go

	//fetch root task from global task queue and expand root task layer by layer, put them into local task queue.
	//Maybe no task puts into local task queue
	bool pull_and_spawn(){
		while(lqueue().size() < THREADS){
			if(lqueue().empty()){
				TaskT* task = NULL;
				//put one task from global task queue to local task queue, make sure global task_queue is not empty
				q_mtx.lock();
				if(!gqueue().empty()){
					task = gqueue().front();
					gqueue().pop();
					task->ltask_queue = ltask_queue;
					task->lq_mtx = &lq_mtx;
				}
				q_mtx.unlock();

				if(task == NULL) return false;
				task->run(false, fouts[0]);
				delete task;
			}
			else{
				stack<TaskT*> tmp_queue;
				tmp_queue.swap(lqueue());
				while(!tmp_queue.empty()){
					TaskT* task = tmp_queue.top();
					tmp_queue.pop();
					task->run(fouts[0]);
					delete task;
				}
			}
		}
		return true;

	}

	bool get_and_process_tasks(ofstream& fout, int batch_size = 1){
		queue<TaskT *> collector;

		lq_mtx.lock();



		while(!lqueue().empty() && batch_size>0){
			TaskT* task = lqueue().top();
			lqueue().pop();
			collector.push(task);
			batch_size--;
		}
		lq_mtx.unlock();

		if(collector.empty()) return false;
		//process tasks in "collector"
		while(!collector.empty()){
			TaskT* task = collector.front();
			collector.pop();
			task->run(fout);
			delete task;
		}

		return true;
	}

	void thread_run(ofstream& fout){
	    while(local_end_label == false) //otherwise, thread terminates
	    {
	        bool busy = get_and_process_tasks(fout);
	        if(!busy) //busy means no task in local queue
	        {
	            unique_lock<mutex> lck(mtx_go);
	            ready_go = false;
	            local_num_idle++;
	            while(!ready_go)
	            {
	                cv_go.wait(lck);
	            }
	            local_num_idle--;
	        }
	    }
	}

	void parallel_run(){
//		bool done = false;
//		while(lqueue().empty()){
//			if(!gqueue2lqueue()){
//				done = true;
//			}
//		}
//		if(done){
//			 ++global_num_idle;
//			 return;
//		}

		//------------------------ create computing threads
	    vector<thread> threads;
	    for(int i=0; i<THREADS; i++)
	    {
	        threads.push_back(thread(&Comper<TaskT>::thread_run, this, ref(fouts[i])));
	    }
	    //------------------------

	    while(local_end_label == false)
	    {
	        usleep(WAIT_TIME_WHEN_IDLE); //avoid busy-checking
	        bool local_empty = false;
	        bool global_empty = false;
	        //------
	        lq_mtx.lock();
        	if(lqueue().empty()) local_empty = true;
	        lq_mtx.unlock();
	        q_mtx.lock();
        	if(gqueue().empty()) global_empty = true;
	        q_mtx.unlock();
	        if(!local_empty)
	        {
	            //case 1: there are tasks to process, wake up threads
	            //the case should go first, since we want to wake up threads early, not till all are idle as in case 2
	            mtx_go.lock();
	            ready_go = true;
	            cv_go.notify_all(); //release threads to compute tasks
	            mtx_go.unlock();
	        }
	        else if(!global_empty){
	    		while(!gqueue2lqueue());
	        }
	        else
	        {
	            mtx_go.lock();
	            if(local_num_idle == THREADS)
	            {
	            	local_end_label = true;
					ready_go = true;
					cv_go.notify_all(); //wake up all thread or release threads to their looping
	            }
	            //case 3: else, some threads are still processing tasks, check in next round
	            mtx_go.unlock();
	        }
	    }
	    //------------------------
	    for(int i=0; i<THREADS; i++) threads[i].join();
        ++global_num_idle;
	}

	//fetch one task from global task queue and push it into local task queue
	bool gqueue2lqueue(){
		TaskT* task = NULL;
		q_mtx.lock();
		if(!gqueue().empty()){
			task = gqueue().front();
			gqueue().pop();
		}
		q_mtx.unlock();
		if(task == NULL) return false;

		task->ltask_queue = ltask_queue;
		task->lq_mtx = &lq_mtx;
		lqueue().push(task);
		//task->run(fouts[0]);
		//delete task;
		return true;
	}
	void run(){ //todo use parallel run directly
		//extend local task queue

		parallel_run();
	}

    void start()
    {
    	main_thread = thread(&Comper::parallel_run, this);
    }

};


#endif /* SYSTEM_COMPER_H_ */

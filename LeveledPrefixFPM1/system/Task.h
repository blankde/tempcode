//########################################################################
//## Copyright 2019 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

//########################################################################
//## Contributors
//## * Wenwen Qu
//## * Da Yan
//########################################################################

#ifndef TASK_H_
#define TASK_H_

#include <stack>
#include <queue>
#include <vector>
#include <mutex>

#include "Global.h" // for task queue
#include "timetrack.h"

template <class PatternT, class ChildrenT, class TransT>
class Task{
public:

	// types
	typedef TransT TransType;
	typedef Task<PatternT, ChildrenT, TransT> TaskT;

	// members
	PatternT pat;
	ChildrenT children; // list of children (often is "map")
	void* ltask_queue;
	mutex* lq_mtx;

	tqueue<TaskT>& gqueue(){ // get the global transaction database
		return *((tqueue<TaskT> *)task_queue);
	}
	stack<Task*>& lqueue(){ // get the global transaction database
		return *(stack<Task*> *)ltask_queue;
	}


	virtual ~Task(){};

	// UDFs
	virtual void setChildren(ChildrenT& children) = 0; // generate new (frequent) children patterns and their PDBs

	virtual Task* get_next_child() = 0; //get next item in "children", need to maintain an iterator in the task object

	virtual bool pre_check(ostream& fout) = 0;
	//{
	//	return true;
	//} // run before setChildren, may output pattern to fout
	// if returns false, will ignore further processing

	virtual bool needSplit() // whether to put new tasks got from get_next_child() into task queue, or process it by current thread
	{
		return true;
	}

	vector<TransT>& TransDB(){ // get the global transaction database
		return *(vector<TransT>*)transDB;
	}

	void run(ostream& fout,bool root = false){ // compute the current task, may generate new tasks to task queue
		if(!pre_check(fout)) return;
		//generate new candidate
		setChildren(children);
		//run new task;
		while(Task* t = get_next_child()){
			// todo stage 2 can delete?
			//stage == 1 means task called by worker, also a root task
			//stage == 2 means task called by comper
			//stage == 3 means task called by thread
			if(root){
				#pragma omp critical
				gqueue().push_back(t); //todo lock-free here??
			}
			else if(needSplit()){ //generate new task, first assign the task queue reference, then push it into local queue. If stage ==3, we need lock local queue first
					t->ltask_queue = this->ltask_queue;
					t->lq_mtx = lq_mtx;
					lq_mtx->lock();
					lqueue().push(t);
					lq_mtx->unlock();
			}
			else{
				t->run(fout);
				delete t;
			}
		}
	}

};

#endif /* TASK_H_ */

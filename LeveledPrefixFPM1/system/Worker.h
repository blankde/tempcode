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

#ifndef WORKER_H_
#define WORKER_H_

#include <stack>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <thread>

#include "Global.h"
#include "Comper.h"
using namespace std;

template<class TaskT>
class Worker {
public:
	typedef typename TaskT::TransType TransT; // can be a pointer, if so, delete transDB elements in destructor

	ifstream fd; // input file stream
	ofstream fout; // @@wenwen: output file for worker
	Comper<TaskT>* compers; // @@wenwen: because mutex is not copyable or moveabel, so not use vector here

	vector<TransT>& TransDB(){ // get the global transaction database
		return *(vector<TransT>*)transDB;
	}

	tqueue<TaskT>& gqueue(){ // get the global transaction database
		return *((tqueue<TaskT> *)task_queue);
	}

	//end tag managed by main thread
	atomic<bool> global_end_label; //end tag, to be set by main thread

	Worker(const char *infile, const char *outfolder = "outputs"){
		//create global transaction database
		transDB = new vector<TransT>;
		//create global transaction database
		task_queue = new tqueue<TaskT>;

		//set global end label to be false. when all (global and local ) task queue is empty and no thread is running, set it to be true
		global_end_label = false;
		global_num_idle = 0; //=== number of idle compers

		//set the input stream
		if (binary_input) { // only supported by Sleuth, not prefixspan or gspan
			fd.open(infile, ios::in | ios::binary);
			if (!fd) {
				cerr << "cannot open input file " << infile << endl;
				exit(1);
			}
		} else {
			fd.open(infile, ios::in);
			if (!fd) {
				cerr << "cannot open input file " << infile << endl;
				exit(1);
			}
		}

		//set the output stream for worker and each compers and threads.
		_mkdir(outfolder);
		fout.open(string(outfolder)+"/0_0");
		compers = new Comper<TaskT>[COMPERS];
		for(int i = 0; i<COMPERS; i++)
			compers[i].init(outfolder+("/"+to_string(i)+"_"));
	};

	virtual void setRoot(tqueue<TaskT>& task_queue) = 0; // put root tasks into task_queue
	// preprocessing code like finding frequent items should also go here

	virtual int getNextTrans(vector<TransT>& transDB) = 0; //to read a transaction from input file stream, and put it into transDB
	// return 0 when reaching end of file

	void read() { //read all lines
		vector<TransT>& db = TransDB();
		while (getNextTrans(db));
	}


	void run() {
		setRoot(gqueue());

		/*expand to COMPERS numbers exactly*/
//		while(tqueue().size() < COMPERS){
//			TaskT* task = tqueue().front();
//			tqueue().pop();
//			task->run(1, fout);
//			delete task;
//		}


		/*expand layer by layer*/
		int layer = 0;
		while(gqueue().size() < F*COMPERS && layer<3){
			vector<TaskT*> tmp_queue;
			tmp_queue.swap(gqueue().task_queue);
			#pragma omp parallel for schedule(dynamic, CHUNK) num_threads(COMPERS*THREADS)
			for(int i = 0; i<tmp_queue.size(); i++){
				TaskT* task = tmp_queue[i];
				task->run(fout,true);
				delete task;
			}
			layer++;
		}

		for(int i =0; i<COMPERS; i++){
			compers[i].start();
		}

		//check whether all compers are finished.
		while(global_end_label == false){
			if(global_num_idle == COMPERS)
				global_end_label = true;
			usleep(WAIT_TIME_WHEN_IDLE); // avoid busy waiting
		}

	}

	virtual ~Worker(){
		delete (vector<TransT>*)transDB;
		delete (tqueue<TaskT> *)task_queue;
		delete[] compers;

		fd.close();
		fout.close();
	}
};

#endif /* WORKER_H_ */

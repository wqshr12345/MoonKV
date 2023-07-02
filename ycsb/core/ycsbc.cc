//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>

#include "utils.h"
#include "timer.h"
#include "client.h"
#include "measurements.h"
#include "core_workload.h"
#include "countdown_latch.h"
#include "db_factory.h"


#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props);

void StatusThread(ycsbc::Measurements *measurements, CountDownLatch *latch, int interval) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;
  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
              << static_cast<long long>(elapsed_time.count()) << " sec: ";

    std::cout << measurements->GetStatusMsg() << std::endl;

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);
  };
}
// #define BACKTRACE_SIZE   16
 
// void dump(void)
// {
// 	int j, nptrs;
// 	void *buffer[BACKTRACE_SIZE];
// 	char **strings;
	
// 	nptrs = backtrace(buffer, BACKTRACE_SIZE);
	
// 	printf("backtrace() returned %d addresses\n", nptrs);
 
// 	strings = backtrace_symbols(buffer, nptrs);
// 	if (strings == NULL) {
// 		perror("backtrace_symbols");
// 		exit(EXIT_FAILURE);
// 	}
 
// 	for (j = 0; j < nptrs; j++)
// 		printf("  [%02d] %s\n", j, strings[j]);
 
// 	free(strings);
// }
 
// void handler(int signo)
// {
	
// // #if 0	
// 	char buff[64] = {0x00};
		
// 	sprintf(buff,"cat /proc/%d/maps", getpid());
		
// 	// system((const char*) buff);
// // #endif	
 
// 	printf("\n=========>>>catch signal %d <<<=========\n", signo);
	
// 	printf("Dump stack start...\n");
// 	dump();
// 	printf("Dump stack end...\n");
 
// 	signal(signo, SIG_DFL); /* 恢复信号默认处理 */
// 	raise(signo);           /* 重新发送信号 */
// }
 
void handler(int sig)
{
    char buff[64] = {0x00};
    sprintf(buff,"cat /proc/%d/maps", getpid());
		
    system((const char*) buff);
    void *array[10];
    size_t size;
 
    // get void*'s for all entries on the stack
    size = backtrace(array, 10);
 
    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d:\n", sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}
int main(const int argc, const char *argv[]) {
  signal(SIGSEGV, handler); /// install our handler
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction = (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  ycsbc::Measurements measurements;
  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads; i++) {
    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, &measurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval = std::stoi(props.GetProperty("status.interval", "10"));

  // load phase
  if (do_load) {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 &measurements, &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, true, true, !do_transaction, &latch));
    }
    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << sum << std::endl;
    std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
  }

  measurements.Reset();
  std::this_thread::sleep_for(std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));

  // transaction phase
  if (do_transaction) {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 &measurements, &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, false, !do_load, true,  &latch));
    }
    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;
  }

  for (int i = 0; i < num_threads; i++) {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 || strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string &message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)" << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  std::cout <<
      "Usage: " << command << " [options]\n"
      "Options:\n"
      "  -load: run the loading phase of the workload\n"
      "  -t: run the transactions phase of the workload\n"
      "  -run: same as -t\n"
      "  -threads n: execute using n threads (default: 1)\n"
      "  -db dbname: specify the name of the DB to use (default: basic)\n"
      "  -P propertyfile: load properties from the given file. Multiple files can\n"
      "                   be specified, and will be processed in the order specified\n"
      "  -p name=value: specify a property to be passed to the DB and workloads\n"
      "                 multiple properties can be specified, and override any\n"
      "                 values in the propertyfile\n"
      "  -s: print status every 10 seconds (use status.interval prop to override)"
      << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}


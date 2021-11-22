    //璁剧疆閲囩敤鍝鍥惧垎鍖虹瓥鐣�
    void set_ingress_method(const std::string& method,
                            size_t bufsize = 50000, bool usehash = false, bool userecent = false,
                            std::string favorite = "source",
                            size_t threshold = 100, size_t theta = 100,size_t etheta=1000, size_t nedges = 0, size_t nverts = 0,size_t ceng=1,
                            size_t interval = std::numeric_limits<size_t>::max()) {
      if(ingress_ptr != NULL) { delete ingress_ptr; ingress_ptr = NULL; }
      if (method == "oblivious") {
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use oblivious ingress, usehash: " << usehash
          << ", userecent: " << userecent << std::endl;
        ingress_ptr = new distributed_oblivious_ingress<VertexData, EdgeData>(rpc.dc(), *this, usehash, userecent);
      } else if  (method == "random") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use random ingress" << std::endl;
        ingress_ptr = new distributed_random_ingress<VertexData, EdgeData>(rpc.dc(), *this); 
      } else if  (method == "matrix_hybrid") {
          if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use matrix ingress" << std::endl;
          ingress_ptr = new distributed_matrix_hybrid_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold);
      } else if  (method == "matrix_strict") {
          if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use matrix ingress" << std::endl;
          ingress_ptr = new distributed_matrix_strict_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold);
      }
      else if  (method == "matrix_ginger") {
          if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use matrix ginger ingress" << std::endl;
          ingress_ptr = new distributed_matrix_ginger_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold, nedges, nverts, interval);
      } else if  (method == "topoX") {
          if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use topoX ingress" << std::endl;
          ingress_ptr = new distributed_topoX_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold,theta,etheta,ceng);
      }else if  (method == "matrix_block") {
          if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use matrix block ingress" << std::endl;
          ingress_ptr = new distributed_matrix_block_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold,theta,etheta,ceng);
      }
      else if (method == "grid") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use grid ingress" << std::endl;
        ingress_ptr = new distributed_constrained_random_ingress<VertexData, EdgeData>(rpc.dc(), *this, "grid");
      } else if (method == "pds") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use pds ingress" << std::endl;
        ingress_ptr = new distributed_constrained_random_ingress<VertexData, EdgeData>(rpc.dc(), *this, "pds");
      } else if (method == "bipartite") {
        if(data_affinity){
          if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use bipartite ingress w/ affinity" << std::endl;
          ingress_ptr = new distributed_bipartite_affinity_ingress<VertexData, EdgeData>(rpc.dc(), *this, favorite);
        } else{
          if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use bipartite ingress w/o affinity" << std::endl;
          ingress_ptr = new distributed_bipartite_random_ingress<VertexData, EdgeData>(rpc.dc(), *this, favorite);
        }
      } else if (method == "bipartite_aweto") {
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use bipartite_aweto ingress" << std::endl;
        ingress_ptr = new distributed_bipartite_aweto_ingress<VertexData, EdgeData>(rpc.dc(), *this, favorite);
      } else if (method == "hybrid") {
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use hybrid ingress" << std::endl;
        ingress_ptr = new distributed_hybrid_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold);
        set_cuts_type(HYBRID_CUTS);
      } else if (method == "hybrid_ginger") {
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use hybrid ginger ingress" << std::endl;
        ASSERT_GT(nedges, 0); ASSERT_GT(nverts, 0);
        ingress_ptr = new distributed_hybrid_ginger_ingress<VertexData, EdgeData>(rpc.dc(), *this, threshold, nedges, nverts, interval);
        set_cuts_type(HYBRID_GINGER_CUTS);
      } else {
        // use default ingress method if none is specified
        std::string ingress_auto = "";
        size_t num_shards = rpc.numprocs();
        int nrow, ncol, p;
        if (sharding_constraint::is_pds_compatible(num_shards, p)) {
          ingress_auto="pds";
          ingress_ptr = new distributed_constrained_random_ingress<VertexData, EdgeData>(rpc.dc(), *this, "pds");
        } else if (sharding_constraint::is_grid_compatible(num_shards, nrow, ncol)) {
          ingress_auto="grid";
          ingress_ptr = new distributed_constrained_random_ingress<VertexData, EdgeData>(rpc.dc(), *this, "grid");
        } else {
          ingress_auto="oblivious";
          ingress_ptr = new distributed_oblivious_ingress<VertexData, EdgeData>(rpc.dc(), *this, usehash, userecent);
        }
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Automatically determine ingress method: " << ingress_auto << std::endl;
      }
      // batch ingress is deprecated
      // if (method == "batch") {
      //   logstream(LOG_EMPH) << "Use batch ingress, bufsize: " << bufsize
      //     << ", usehash: " << usehash << ", userecent" << userecent << std::endl;
      //   ingress_ptr = new distributed_batch_ingress<VertexData, EdgeData>(rpc.dc(), *this,
      //                                                    bufsize, usehash, userecent);
      // } else 
    } // end of set ingress method







---------------------------------------------------
    void set_options(const graphlab_options& opts) {
      std::string ingress_method = "";

      // hybrid cut
      size_t threshold = 100;
        //topox
        size_t theta = 100;
        size_t etheta = 1000;
        size_t ceng = 1;
      // ginger heuristic
      size_t interval = std::numeric_limits<size_t>::max();
      size_t nedges = 0;
      size_t nverts = 0;
      // bipartite
      std::string favorite = "source"; /* source or target */

      
      // deprecated
      size_t bufsize = 50000;
      bool usehash = false;
      bool userecent = false;

      std::vector<std::string> keys = opts.get_graph_args().get_option_keys();
      foreach(std::string opt, keys) {
        if (opt == "ingress") {
          opts.get_graph_args().get_option("ingress", ingress_method);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: ingress = "
              << ingress_method << std::endl;
        } else if (opt == "parallel_ingress") {
         opts.get_graph_args().get_option("parallel_ingress", parallel_ingress);
          if (!parallel_ingress && rpc.procid() == 0)
            logstream(LOG_EMPH) << "Disable parallel ingress. Graph will be streamed through one node."
              << std::endl;
        } else if (opt == "threshold") {
          opts.get_graph_args().get_option("threshold", threshold);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: threshold = "
                                << threshold << std::endl;
        }else if (opt == "ceng") {
            opts.get_graph_args().get_option("ceng", ceng);
            if (rpc.procid() == 0)
                logstream(LOG_EMPH) << "Graph Option: ceng = "
                                    << ceng << std::endl;
        }
        else if (opt == "theta") {
            opts.get_graph_args().get_option("theta", theta);
            if (rpc.procid() == 0)
                logstream(LOG_EMPH) << "Graph Option: theta = "
                                    << theta << std::endl;
        }
        else if (opt == "etheta"){
            opts.get_graph_args().get_option("etheta", etheta);
            if (rpc.procid() == 0)
                logstream(LOG_EMPH) << "Graph Option: etheta = "
                                    << etheta << std::endl;
        }
        else if (opt == "interval") {
          opts.get_graph_args().get_option("interval", interval);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: interval = "
                                << interval << std::endl;
        }  else if (opt == "nedges") {
          opts.get_graph_args().get_option("nedges", nedges);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: nedges = "
                                << nedges << std::endl;
        } else if (opt == "nverts") {
          opts.get_graph_args().get_option("nverts", nverts);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: nverts = "
                                << nverts << std::endl;
        } else if (opt == "affinity") {
          opts.get_graph_args().get_option("affinity", data_affinity);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: affinity = "
                                << data_affinity << std::endl;
        } else if (opt == "favorite") {
          opts.get_graph_args().get_option("favorite", favorite);
          if(favorite != "target") favorite = "source";
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: favorite = "
                                << favorite << std::endl;
        }
        
        /**
         * These options below are deprecated.
         */
        else if (opt == "bufsize") {
          opts.get_graph_args().get_option("bufsize", bufsize);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: bufsize = "
              << bufsize << std::endl;
        } else if (opt == "usehash") {
          opts.get_graph_args().get_option("usehash", usehash);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: usehash = "
              << usehash << std::endl;
        } else if (opt == "userecent") {
          opts.get_graph_args().get_option("userecent", userecent);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: userecent = "
              << userecent << std::endl;
        }
	else if (opt == "partitionfile") {
          opts.get_graph_args().get_option("partitionfile", partitionfile);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: partitionfile = "
              << partitionfile << std::endl;
        }else{
          logstream(LOG_ERROR) << "Unexpected Graph Option: " << opt << std::endl;
        }
      }
        set_ingress_method(ingress_method, bufsize, usehash, userecent, favorite,
                           threshold, theta,etheta, nedges, nverts, ceng, interval);
    }



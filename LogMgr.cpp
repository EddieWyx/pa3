#include "LogMgr.h"
#include <sstream>
#include "../StorageEngine/StorageEngine.h"
#include <climits>
#include <algorithm>
#include <iostream>

using namespace std;

int LogMgr::getLastLSN(int txnum){
  // find the most recent tx from the tx_table
  if(tx_table.find(txnum) == tx_table.end()){
    cout<<"getLastLSN: no txnum"<<endl;
    return NULL_LSN;
  }else{
    cout<<"getLastLSN: txnum"<<endl;
    return tx_table[txnum].lastLSN;
  }
}

void LogMgr::setLastLSN(int txnum, int lsn){
  if(getLastLSN(txnum) != NULL_LSN){
    cout<<"setLastLSN: not null"<<endl;
    // means has this txnum then just update
    tx_table[txnum].lastLSN = lsn; 
  }else{
    // we need to create the new txTableEntry
    cout<<"setLastLSN: create a new one"<<endl;
    txTableEntry ntxTE(lsn,U);
    tx_table[txnum] = ntxTE;
  }
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
*/
void LogMgr::flushLogTail(int maxLSN){
  int siz = logtail.size();
  int i = 0;
  // if this is samller than or equal the maxLSN
  while(logtail[i]->getLSN() <= maxLSN && i<siz){
    // flash the log to the disk
    cout<<"flushLogTail: flash to the disk"<<endl;
    se->updateLog(logtail[i]->toString());
    i++;
  }
  if(i>0){
    // erase the log from the logtail
    cout<<"flushLogTail: erase the log from logtail"<<endl;
    logtail.erase(logtail.begin(),logtail.begin()+i);
  }
}
/* 
 * Run the analysis phase of ARIES.
 */
void LogMgr::analyze(vector <LogRecord*> log){
  // find the most recent check_begin if there is one
  int B_CKPT = NULL_LSN;
  int E_CKPT = NULL_LSN;
  
  for(int i = log.size() - 1;i >=0; i--){
    if(log[i]->getType() == BEGIN_CKPT){
      B_CKPT =i;
      break;
    }
    else if(log[i]->getType() == END_CKPT){
      E_CKPT = i;
    }
  }
  
  // if there is begin checkpoint
  if(B_CKPT != NULL_LSN){
    ChkptLogRecord * end_CKPT_Pointer = (ChkptLogRecord *)log[E_CKPT];
    tx_table = end_CKPT_Pointer->getTxTable();
    dirty_page_table = end_CKPT_Pointer->getDirtyPageTable();
  }


  // modifying the tx and dirty table from the begin check point
  for(int i = B_CKPT + 1; i< log.size() ; i++){
    int lsn = log[i]->getLSN();
    // skip the end LSN
    if(lsn != E_CKPT){
      int pre_lsn = log[i]->getprevLSN();
      int txid = log[i]->getTxID();
      TxType t = log[i]->getType();
      
      map<int,txTableEntry>::iterator it;
      map<int,int>::iterator dit;
      it = tx_table.find(txid);
      // end type
      if(t == END){
        if(it != tx_table.end()){
          tx_table.erase(it);
        }
      }
      //other than end
      else{
        
        if(it != tx_table.end()){
          tx_table[txid].lastLSN = lsn;
        }else{
          //adding new 
          txTableEntry t(lsn,TxStatus::U);
          tx_table[txid] = t;
        }

        if(t == COMMIT){
          tx_table[txid].status = TxStatus::C;
        }
        
        else if( t == ABORT){
          tx_table[txid].status = TxStatus::U;
        }

        else if( t == UPDATE ){
          tx_table[txid].status = TxStatus::U;
          UpdateLogRecord * updLog = (UpdateLogRecord * ) log[i];
          int pageID = updLog->getPageID();
          dit = dirty_page_table.find(pageID);
          // if not found in the dirty dtale
          if(dit == dirty_page_table.end()){
            dirty_page_table[pageID] = lsn;
          }
        }else if(t == CLR){
          tx_table[txid].status = TxStatus::U;
          CompensationLogRecord * clrLog = (CompensationLogRecord * ) log[i];
          int pageID = clrLog->getPageID();
          dit = dirty_page_table.find(pageID);
          // if not found in the dirty dtale
          if(dit == dirty_page_table.end()){
            dirty_page_table[pageID] = lsn;
          }
        }else{
          cout<<"Something Wrong in the analysis"<<endl;
        }

      }
    }
  }
}

/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding, return false.
 * Else when redo phase is complete, return true. 
 */
bool LogMgr::redo(vector <LogRecord*> log){
  //first find the oldest update in the dirty-table
  int oldest_lsn = INT_MAX;
  map<int,int>::iterator it;
  for(it = dirty_page_table.begin(); it!=dirty_page_table.end(); it++){
    if(it->second < oldest_lsn){
      oldest_lsn = it->second;
    }
  }
  // iterate through the log 
  for(int i = oldest_lsn; i<log.size() ; i++){
    TxType t = log[i]->getType();
    if(t == UPDATE || t == CLR){
      if(t == UPDATE){
        // if this is update
        UpdateLogRecord * updLog = (UpdateLogRecord * ) log[i];
        int pageID = updLog->getPageID();
        //check the constraint 
        it = dirty_page_table.find(pageID);
        //if the pageID is int the dirty page
        if(it !=dirty_page_table.end()){
          // check if the reclsn is samller then the lsn
          int lsn = updLog->getLSN();
          int reclsn = dirty_page_table[pageID]; 
          if(reclsn <= lsn){
            //check the page lsn is samller than the lsn
            int pageLSN = se->getLSN(pageID);
            if(pageLSN < lsn){
              // redo things
              if(se->pageWrite(pageID,updLog->getOffset(),updLog->getAfterImage(),lsn)){

              }else{
                return false;
              }
            }
          }
        }
      }
      else{
        // if this is clr
        CompensationLogRecord * clrLog = (CompensationLogRecord *) log[i];
        int pageID = clrLog->getPageID();
        it = dirty_page_table.find(pageID);
        if(it != dirty_page_table.end()){
          int lsn = clrLog->getLSN();
          int reclsn = dirty_page_table[pageID]; 
          if(reclsn <= lsn){
            int pageLSN = se->getLSN(pageID);
            if(pageLSN < lsn){
              if(se->pageWrite(pageID,clrLog->getOffset(),clrLog->getAfterImage(),lsn)){

              }else{
                return false;
              }
            }
          }
        }
      }
    }
  }
  // write end type record for C type
  map<int ,txTableEntry>::iterator tit;
  for(tit = tx_table.begin(); tit != tx_table.end(); tit++){
    if(tit->second.status == C){
      LogRecord* eLR = new LogRecord(se->nextLSN(),tit->second.lastLSN,tit->first,TxType::END);
      logtail.push_back(eLR);
      tx_table.erase(tit);
    }
  }

  return false;
}

/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
// void LogMgr::undo(vector <LogRecord*> log, int txnumc){
//   if(txnumc == NULL_TX){
//     cout<<"statring undo phase of ARIES"<<endl;


//   }else{
//     cout<<"statring undo a specific transaction"<<endl;
//   }
// }

void LogMgr::undo(vector<LogRecord *> log, int txnum){
   if(txnum == NULL_TX){
     //undo

     vector<int> ToUndo;
     for(auto i = tx_table.begin(); i != tx_table.end(); i++){
         pair<int, txTableEntry> p = *i;
         if(p.second.status == U){
            ToUndo.push_back(p.second.lastLSN);
         }
      }
     int max_idx = log.size()-1;
     while(!ToUndo.empty()){
       //vector<int>::iterator pick_max_lsn = max_element(ToUndo.begin(), ToUndo.end());
       //int max_lsn = *pick_max_lsn;

       int max_lsn = INT_MIN;
       int max_lsn_idx = -1;
       for(int i = 0; i < ToUndo.size(); i++){
          if(ToUndo[i] > max_lsn)
          {max_lsn = ToUndo[i];
           max_lsn_idx = i; 
          }
       }
       //undo 
       for(int i = max_idx; i >= 0; i--){
         //find max lsn in log
         if(log[i]->getLSN() == max_lsn){
            // if it is an update lsn
            if(log[i]->getType() == UPDATE){
               LogRecord * lr = log[i];
               UpdateLogRecord * ulr_ptr = dynamic_cast<UpdateLogRecord *>(lr);

               //write a CLR log
               int new_lsn = se->nextLSN(); 
               CompensationLogRecord *clr_ptr = new CompensationLogRecord(new_lsn, getLastLSN(ulr_ptr->getTxID()), ulr_ptr->getTxID(), ulr_ptr->getPageID(), ulr_ptr->getOffset(), ulr_ptr->getBeforeImage(), ulr_ptr->getprevLSN()); //it is befor image in book
               logtail.push_back(clr_ptr);

               //update lastLSN in tx_table
               setLastLSN(ulr_ptr->getTxID(), new_lsn);

                //???new_lsn or log[i]->previous_lsn???or log[i]->getLSN()???
               if (!se->pageWrite(ulr_ptr->getPageID(), ulr_ptr->getOffset(), ulr_ptr->getBeforeImage(), new_lsn))
                  return;

         //do we need to update dirty page now? if yes, making change both here and abort, see piazza
               if(dirty_page_table.find(ulr_ptr->getPageID()) == dirty_page_table.end()){
                  pair<int, int> p(ulr_ptr->getPageID(), new_lsn);
                  dirty_page_table.insert(p);
               }

               if(log[i]->getprevLSN() != NULL_LSN)
                 ToUndo.push_back(log[i]->getprevLSN());
               else{
                 //write an end
                 LogRecord *end_lr_ptr = new LogRecord(se->nextLSN(), getLastLSN(log[i]->getTxID()), ulr_ptr->getTxID(), END);
                 logtail.push_back(end_lr_ptr);
                 //remove the tx from tx_table
                 tx_table.erase(ulr_ptr->getTxID());
               }

               
            }
            else if(log[i]->getType() == CLR){
               LogRecord * lr = log[i];
               CompensationLogRecord * clr_ptr = dynamic_cast<CompensationLogRecord *>(lr);
               if(clr_ptr->getUndoNextLSN() != NULL_LSN)
                 ToUndo.push_back(clr_ptr->getUndoNextLSN());
               else{
               //write an end log 
               int previousLSN = tx_table[log[i]->getTxID()].lastLSN;
               LogRecord * end_lr_ptr = new LogRecord(se->nextLSN(), previousLSN, log[i]->getTxID(), END);
               //remove this tx from tx_table
               tx_table.erase(log[i]->getTxID());
               //leave it in the logtail
               for(int i = 0; i < logtail.size(); i++){
                if(logtail[i]->getLSN() == log[i]->getLSN())
                  logtail.erase(logtail.begin()+i);
               }
               }
            }
            max_idx = i;  //next find starts from max_idx
            ToUndo.erase(ToUndo.begin()+max_lsn_idx);
            break;
         }
       }

     }
   }
   else{
     //txnum is provided, abort that transaction
     if(tx_table.find(txnum) == tx_table.end())
     {
       return;
     }
     int lastLSN = tx_table[txnum].lastLSN; //get the most recent lsn from tx_table

     int ab_new_lsn = se->nextLSN();
     //write an abort log
     LogRecord * ab_lr_ptr = new LogRecord(ab_new_lsn, lastLSN, txnum, ABORT);
     //update tx_table 
     setLastLSN(txnum, ab_new_lsn);
     logtail.push_back(ab_lr_ptr);

     int max_idx = log.size()-1;
     vector<int> ToUndo;
     ToUndo.push_back(lastLSN);

     while(!ToUndo.empty()){
       int max_lsn = INT_MIN;
       int max_lsn_idx = -1;
       for(int i = 0; i < ToUndo.size(); i++){
          if(ToUndo[i] > max_lsn)
          {max_lsn = ToUndo[i];
           max_lsn_idx = i; 
          }
       }
       //undo 
       for(int i = max_idx; i >= 0; i--){
         //find max lsn in log
         if(log[i]->getLSN() == max_lsn){
            // if it is an update lsn
            if(log[i]->getType() == UPDATE){
               LogRecord * lr = log[i];
               UpdateLogRecord * ulr_ptr = dynamic_cast<UpdateLogRecord *>(lr);

               //write a CLR log
               int new_lsn = se->nextLSN(); 
               CompensationLogRecord *clr_ptr = new CompensationLogRecord(new_lsn, getLastLSN(ulr_ptr->getTxID()), ulr_ptr->getTxID(), ulr_ptr->getPageID(), ulr_ptr->getOffset(), ulr_ptr->getBeforeImage(), ulr_ptr->getprevLSN()); //why after image???
               //update lastLSN in tx_table
               setLastLSN(ulr_ptr->getTxID(), new_lsn);

               logtail.push_back(clr_ptr); //put in log or logtail???

                //???new_lsn or log[i]->previous_lsn???or log[i]->getLSN()???
               if(!se->pageWrite(ulr_ptr->getPageID(), ulr_ptr->getOffset(), ulr_ptr->getBeforeImage(), new_lsn))
                  return;

               // make changes here if we need to update DPT 
              if(dirty_page_table.find(ulr_ptr->getPageID()) == dirty_page_table.end()){
                  pair<int, int> p(ulr_ptr->getPageID(), new_lsn);
                  dirty_page_table.insert(p);
               }

               if(log[i]->getprevLSN() != NULL_LSN)
                 ToUndo.push_back(log[i]->getprevLSN());
               else{
                 //write an end
                 LogRecord *end_lr_ptr = new LogRecord(se->nextLSN(), getLastLSN(log[i]->getTxID()), ulr_ptr->getTxID(), END);
                 logtail.push_back(end_lr_ptr);
                 //remove the tx from tx_table
                 tx_table.erase(ulr_ptr->getTxID());
               }

               
            }
            else if(log[i]->getType() == CLR){
               LogRecord * lr = log[i];
               CompensationLogRecord * clr_ptr = dynamic_cast<CompensationLogRecord *>(lr);
               if(clr_ptr->getUndoNextLSN() != NULL_LSN)
                 ToUndo.push_back(clr_ptr->getUndoNextLSN());
               else{
               //write an end log 
               int previousLSN = tx_table[log[i]->getTxID()].lastLSN;
               LogRecord * end_lr_ptr = new LogRecord(se->nextLSN(), previousLSN, log[i]->getTxID(), END);
               //remove this tx from tx_table
               tx_table.erase(log[i]->getTxID());
               //leave it in the logtail
               for(int i = 0; i < logtail.size(); i++){
                if(logtail[i]->getLSN() == log[i]->getLSN())
                  logtail.erase(logtail.begin()+i);
               }
               }
            }
            max_idx = i;  //next find starts from max_idx
            break;
         }
       }
            ToUndo.erase(ToUndo.begin() + max_lsn_idx);

     }
   }
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
  vector<LogRecord*> result;
  istringstream stream(logstring);
  string line;
  while(getline(stream, line)) {
    LogRecord* lr = LogRecord::stringToRecordPtr(line);
    result.push_back(lr);
  }
  return result;
}

/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid){
  // write the abort log
  int nLSN = se->nextLSN();
  TxType a = ABORT;
  LogRecord * aRecord = new LogRecord(nLSN,getLastLSN(txid),txid,a);
  logtail.push_back(aRecord);

  string newLog = se->getLog();
  vector<LogRecord*> temp = stringToLRVector(newLog);
  undo(temp,txid);
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void LogMgr::checkpoint(){
  // first write the begin checkpoint log
  int bckLSN = se->nextLSN();
  TxType bck = BEGIN_CKPT;
  LogRecord * bckRecord = new LogRecord(bckLSN,NULL_LSN,NULL_TX,bck);
  logtail.push_back(bckRecord);
  // write the end checkpoint to the log
  int eckLSN = se->nextLSN();
  ChkptLogRecord * eckRecord = new ChkptLogRecord(eckLSN,bckLSN,NULL_TX,tx_table,dirty_page_table);
  logtail.push_back(eckRecord);
  flushLogTail(eckLSN);
  // writet the masterLSN;
  if(se->store_master(bckLSN)){
    cout<<"finish the checkpoint"<<endl;
  }
}

/*
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid){
  // write this log record to the logtail
  int nLSN = se->nextLSN();
  TxType t = COMMIT;
  LogRecord * cRecord = new LogRecord(nLSN,getLastLSN(txid),txid,t);
  logtail.push_back(cRecord);

  // adding to the trasction table since other than the end
  TxStatus s = C;
  txTableEntry txEntry(nLSN,s);
  tx_table[txid] = txEntry;
  // flash tail
  flushLogTail(nLSN);
  // remove the tx
  map<int,txTableEntry>::iterator it;
  it = tx_table.find(txid);
  if(it!=tx_table.end()){
    tx_table.erase(t);
  }
  //write the end
  int eLSN = se->nextLSN();
  TxType e = END;
  LogRecord * eRecord = new LogRecord(eLSN,getLastLSN(txid),txid,e);
  logtail.push_back(eRecord);
}

/*
 * A function that StorageEngine will call when it's about to 
 * write a page to disk. 
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id){
  // first force all the update log with this page_id (I guess from the logtail)
  // to the disk;
  flushLogTail(se->getLSN(page_id));

  // after push the page to the disk
  // remove the page from the dirty table
  map<int,int>::iterator it;
  it = dirty_page_table.find(page_id);
  if(it!=dirty_page_table.end()){
    dirty_page_table.erase(it);
  }
}

/*
 * Recover from a crash, given the log from the disk.
 */
void LogMgr::recover(string log){
  //convert to the LogRecord*
  vector<LogRecord*> res = stringToLRVector(log);
  analyze(res);
  if(redo(res)){
    undo(res);
  }else{
    return;
  }
}

/*
 * Logs an update to the database and updates tables if needed.
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
  int pageLSN = se->nextLSN();
  UpdateLogRecord* neWrt =  
    new UpdateLogRecord(pageLSN, getLastLSN(txid),txid,page_id,offset,oldtext,input);
  // adding to the logtail
  logtail.push_back(neWrt);
  // update the table
  setLastLSN(txid,pageLSN);
  // update the dirtypage table 
  if(dirty_page_table.find(page_id) == dirty_page_table.end()){
    dirty_page_table[page_id] = pageLSN;
  }
  return pageLSN;
}

/*
 * Sets this.se to engine. 
 */
void LogMgr::setStorageEngine(StorageEngine* engine){
  this->se = engine;
}







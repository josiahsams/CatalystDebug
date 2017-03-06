package org.apache.spark

/**
  * Created by madhusudanan on 2/23/17.
  */
object DebugMetrics {

  case class jobdetails(query: String, taskType : String, jobId : Int, stageId : Int)
  val s = new ThreadLocal[jobdetails];

  def get() = {
    if(s.get() == null)
      jobdetails("unknown","unknown",-1,-1)
    else
      s.get()
  }

  def set(query : String, taskType : String, jobId : Int, stageId : Int) = {
    s.set(jobdetails(query,taskType,jobId,stageId))
  }

  def getTaskType() = get().taskType
  def getJobId() = get.jobId
  def getStageId() = get.stageId
  def getQuery() = get().query

}

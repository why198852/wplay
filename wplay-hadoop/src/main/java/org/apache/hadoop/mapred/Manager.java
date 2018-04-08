//package org.apache.hadoop.mapred;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hdfs.protocol.ClientProtocol;
//import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
//import org.apache.hadoop.hdfs.protocol.FSConstants;
//import org.apache.hadoop.hdfs.server.namenode.NameNode;
//import org.apache.hadoop.ipc.RPC;
//
///**
// *
// * @author James
// *
// */
//public class Manager {
//	private JobSubmissionProtocol jobProtocol;
//	private ClientProtocol clientProtocol;
//	public static final int TASK_ACTIVE = 0;
//	public static final int TASK_STOP = -1;
//
//	/**
//	 *
//	 * @param conf
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	public Manager(Configuration conf) throws IOException, InterruptedException {
//
////		this.jobProtocol = ((JobSubmissionProtocol) RPC.getProxy(
////				JobSubmissionProtocol.class, JobSubmissionProtocol.versionID,
////				JobTracker.getAddress(conf), conf));
//
//		// for cdh
//		this.clientProtocol= RPC.getProtocolProxy(ClientProtocol.class,
//				ClientProtocol.versionID, NameNode.getAddress(conf), conf)
//				.getProxy();
//		System.out.println(clientProtocol.getStats().length);
//
//		// test by apache hadoop cluster
//		// this.clientProtocol = (ClientProtocol) RPC.getProxy(
//		// ClientProtocol.class, 61L, NameNode.getAddress(conf), conf);
//
//	}
//
//	/**
//	 * Get a set of statistics about the filesystem. Right now, only three
//	 * values are returned.
//	 * <ul>
//	 * <li>[0] contains the total storage capacity of the system, in bytes.</li>
//	 * <li>[1] contains the total used space of the system, in bytes.</li>
//	 * <li>[2] contains the available storage of the system, in bytes.</li>
//	 * <li>[3] contains number of under replicated blocks in the system.</li>
//	 * <li>[4] contains number of blocks with a corrupt replica.</li>
//	 * <li>[5] contains number of blocks without any good replicas left.</li>
//	 * </ul>
//	 * Use public constants like  in place of
//	 * actual numbers to index into the array.
//	 *
//	 * @throws IOException
//	 */
//	public long[] getStats() throws IOException {
//
//		return clientProtocol.getStats();
//	}
//
//	/**
//	 * Get a report on the system's current datanodes. One DatanodeInfo object
//	 * is returned for each DataNode. Return live datanodes if type is LIVE; .
//	 */
//	public DatanodeInfo[] getLiveDatanode() throws IOException {
//		return clientProtocol
//				.getDatanodeReport(FSConstants.DatanodeReportType.LIVE);
//	}
//
//	/**
//	 * Get a report on the system's current datanodes. One DatanodeInfo object
//	 * is returned for each DataNode. Return dead datanodes if type is DEAD; .
//	 */
//	public DatanodeInfo[] getDeadDatanode() throws IOException {
//		return clientProtocol
//				.getDatanodeReport(FSConstants.DatanodeReportType.DEAD);
//	}
//
//	/**
//	 * Gets set of Job Queues associated with the Job Tracker
//	 *
//	 * @return Array of the Job Queue Information Object
//	 * @throws IOException
//	 */
//	public JobQueueInfo[] getQueues() throws IOException {
//		return jobProtocol.getQueues();
//
//	}
//
//	/**
//	 * Grab the current job counters
//	 */
//	public Counters getJobCounters(JobID jobid) throws IOException {
//		return jobProtocol.getJobCounters(jobid);
//	}
//
//	/**
//	 * Grab the jobtracker system directory path where job-specific files are to
//	 * be placed.
//	 *
//	 * @return the system directory where job-specific files are to be placed.
//	 */
//	public String getSystemDir() {
//		return jobProtocol.getSystemDir();
//	}
//
//	/**
//	 * Get the current status of the cluster
//	 *
//	 * @param detailed
//	 *            if true then report tracker names and memory usage
//	 * @return summary of the state of the cluster
//	 */
//	public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
//		return jobProtocol.getClusterStatus(detailed);
//	}
//
//	/**
//	 * Grab a bunch of info on the map tasks that make up the job
//	 *
//	 * @param jobid
//	 * @return
//	 * @throws IOException
//	 */
//	public TaskReport[] getMapTaskReports(JobID jobid) throws IOException {
//		return jobProtocol.getMapTaskReports(jobid);
//	}
//
//	/**
//	 * Grab a bunch of info on the reduce tasks that make up the job
//	 *
//	 * @param jobid
//	 * @return
//	 * @throws IOException
//	 */
//	public TaskReport[] getReduceTaskReports(JobID jobid) throws IOException {
//		return jobProtocol.getReduceTaskReports(jobid);
//	}
//
//	/**
//	 * Grab a bunch of info on the cleanup tasks that make up the job
//	 *
//	 * @param jobid
//	 * @return
//	 * @throws IOException
//	 */
//	public TaskReport[] getCleanupTaskReports(JobID jobid) throws IOException {
//		return jobProtocol.getCleanupTaskReports(jobid);
//	}
//
//	/**
//	 * Grab a bunch of info on the setup tasks that make up the job
//	 *
//	 * @param jobid
//	 * @return
//	 * @throws IOException
//	 */
//	public TaskReport[] getSetupTaskReports(JobID jobid) throws IOException {
//		return jobProtocol.getSetupTaskReports(jobid);
//	}
//
//	/**
//	 *
//	 * @param jobid
//	 * @return JobProfile
//	 * @throws IOException
//	 */
//	public JobProfile getJobProfile(JobID jobid) throws IOException {
//		return this.jobProtocol.getJobProfile(jobid);
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public JobStatus[] getAllJobs() throws IOException {
//		return this.jobProtocol.getAllJobs();
//	}
//
//	/**
//	 *
//	 * @param jobId
//	 * @throws IOException
//	 */
//	public void killJob(String jobId) throws IOException {
//		killJob(getJobId(jobId));
//	}
//
//	/**
//	 *
//	 * @param jobID
//	 * @throws IOException
//	 */
//	public void killJob(JobID jobID) throws IOException {
//		this.jobProtocol.killJob(jobID);
//	}
//
//	/**
//	 *
//	 * @param jobID
//	 * @return
//	 */
//	private JobID getJobId(String jobID) {
//		return JobID.forName(jobID);
//	}
//
//	/**
//	 *
//	 * @param jobId
//	 * @return
//	 * @throws IOException
//	 */
//	public JobStatus getJobStatus(String jobId) throws IOException {
//		return this.jobProtocol.getJobStatus(getJobId(jobId));
//	}
//
//	/**
//	 *
//	 * @param job
//	 * @throws IOException
//	 */
//	public void killJob(JobStatus job) throws IOException {
//		killJob(job.getJobID());
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public JobStatus[] getRunningJob() throws IOException {
//		return this.jobProtocol.jobsToComplete();
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public List<JobStatus> getSucceededJob() throws IOException {
//		return getJobs(2);
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public List<JobStatus> getFailedJob() throws IOException {
//		return getJobs(3);
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public List<JobStatus> getPrepJob() throws IOException {
//		return getJobs(4);
//	}
//
//	/**
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	public List<JobStatus> getKilledJob() throws IOException {
//		return getJobs(5);
//	}
//
//	/**
//	 *
//	 * @param status
//	 * @return
//	 * @throws IOException
//	 */
//	private List<JobStatus> getJobs(int status) throws IOException {
//		List<JobStatus> jobs = new ArrayList<JobStatus>();
//		JobStatus[] allJobs = getAllJobs();
//		for (JobStatus job : allJobs) {
//			if (job.getRunState() == status) {
//				jobs.add(job);
//			}
//		}
//		return reList(jobs);
//	}
//
//	/**
//	 *
//	 * @param jobs
//	 * @return
//	 */
//	private List<JobStatus> reList(List<JobStatus> jobs) {
//		return jobs.isEmpty() ? null : jobs;
//	}
//
//	public static void main(String[] arg0) {
////		 Configuration conf = XmlConfigUtil.create();
//		// // Configuration conf = new Configuration();
//		// // InetSocketAddress addr = new InetSocketAddress("master", 9000); //
//		// the
//		// // // server's
//		// // // inetsocketaddress
//		// // try {
//		// // ClientProtocol client = (ClientProtocol) RPC.waitForProxy(
//		// // ClientProtocol.class, 61, addr, conf);
//		// // DatanodeInfo[] d = client
//		// // .getDatanodeReport(FSConstants.DatanodeReportType.LIVE);
//		// // System.out.println(d.length);
//		// // for (int i = 0; i < d.length; i++) {
//		// // DatanodeInfo node = d[i];
//		// // System.out.println(node.getHostName() + " -- " + node.name +
//		// " -- "
//		// // + node.getCapacity() + " -- " + node.getDfsUsed());
//		// // }
//		// //
//		// // } catch (IOException e) {
//		// // // TODO Auto-generated catch block
//		// // e.printStackTrace();
//		// // }
////		 Manager manager;
////		 try {
////		 manager = new Manager(conf);
////		 DatanodeInfo[] datanodes = manager.getLiveDatanode();
////		 System.out.println(datanodes.length);
////		 } catch (IOException e) {
////		 // TODO Auto-generated catch block
////		 e.printStackTrace();
////		 } catch (InterruptedException e) {
////		 // TODO Auto-generated catch block
////		 e.printStackTrace();
////		 }
//
//	}
//}
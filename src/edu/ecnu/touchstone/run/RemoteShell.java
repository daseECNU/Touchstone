package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;

public class RemoteShell {

	//execute a shell command on the target machine
	public static String exec(String ip, String userName, String passwd, String cmd) {
		Connection conn = null;
		InputStream in = null;
		String result = null;
		try {
			conn = new Connection(ip);
			conn.connect();
			if (conn.authenticateWithPassword(userName, passwd)) {
				Session session = conn.openSession();
				session.execCommand(cmd);
				in = session.getStdout();
				result = processStdout(in, Charset.defaultCharset().toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	// upload a local file to remote target directory
	public static void uploadFile(String ip, String userName, String passwd, 
			String localFile, String remoteTargetDirectory) {
		Connection conn = null;
		try {
			conn = new Connection(ip);
			conn.connect();
			if (conn.authenticateWithPassword(userName, passwd)) {
				SCPClient scpClient = conn.createSCPClient();
				scpClient.put(localFile, remoteTargetDirectory);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
	}
	
	// upload a local directory to remote target directory
	public static void uploadDirectory(String ip, String userName, String passwd, 
			String localDirectory, String remoteTargetDirectory) {
		Connection conn = null;
		try {
			conn = new Connection(ip);
			conn.connect();
			if (conn.authenticateWithPassword(userName, passwd)) {
				int index1 = localDirectory.lastIndexOf("/");
				int index2 =  localDirectory.lastIndexOf("\\");
				int index = index1 > index2 ? index1 : index2;
				String localDirectoryName = localDirectory.substring(index + 1);
				remoteTargetDirectory = remoteTargetDirectory + "//" + localDirectoryName;
				exec(ip, userName, passwd, "rm -rf " + remoteTargetDirectory + 
						" \n mkdir -p " + remoteTargetDirectory);
				
				SCPClient scpClient = conn.createSCPClient();
				File[] localFiles = new File(localDirectory).listFiles();
				for (int i = 0; i < localFiles.length; i++) {
					if (localFiles[i].isDirectory()) {
						uploadDirectory(ip, userName, passwd, localFiles[i].getAbsolutePath(), remoteTargetDirectory);
					} else {
						scpClient.put(localFiles[i].getAbsolutePath(), remoteTargetDirectory);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
	}

	// download a remote file to a local target directory
	public static void downloadFile(String ip, String userName, String passwd, 
			String remoteFile, String localTargetDirectory) {
		Connection conn = null;
		try {
			conn = new Connection(ip);
			conn.connect();
			if (conn.authenticateWithPassword(userName, passwd)) {
				SCPClient scpClient = conn.createSCPClient();
				scpClient.get(remoteFile, localTargetDirectory);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
	}

	//convert byte stream into character stream
	private static String processStdout(InputStream in, String charset) {
		StringBuffer sb = new StringBuffer();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in, charset));
			String inputLine = br.readLine();
			while (inputLine != null) {
				sb.append(inputLine + "\n");
				inputLine = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	// test
	public static void main(String[] args) throws Exception{
//		String result = RemoteShell.exec("10.11.1.190", "touchstone", "123456", "ls \n ls");
//		System.out.println(result);
//		
//		RemoteShell.uploadFile("10.11.1.190", "touchstone", "123456", ".//test//touchstone.conf", "~//");
//		RemoteShell.downloadFile("10.11.1.190", "touchstone", "123456", "~//touchstone.conf", ".//");
//		
//		RemoteShell.uploadDirectory("10.11.1.190", "touchstone", "123456", ".//test//input", "~//");
//		RemoteShell.uploadDirectory("10.11.1.190", "touchstone", "123456", ".//test", "~//");
		
		RemoteShell.exec(
				"10.11.1.192", "root", "Ecn5d@se", 
				"sync; sync; sync; echo 3 > /proc/sys/vm/drop_caches \n " + 
				"echo 0 > /proc/sys/vm/drop_caches");
	}
} 


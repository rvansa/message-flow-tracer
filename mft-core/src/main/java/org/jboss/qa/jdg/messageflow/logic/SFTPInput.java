/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.qa.jdg.messageflow.logic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SFTPInput implements Input {
   private final String host;
   private final String file;
   private final int port;
   private final String username;
   private SSHClient ssh;
   private SFTPClient sftp;
   private volatile boolean run = true;
   private RemoteFile.RemoteFileInputStream remoteStream;
   private PipedOutputStream bufferedOutput;
   private PipedInputStream bufferedInput;
   private volatile IOException thrown;
   private Thread preReader;

   public SFTPInput(String username, String host, int port, String file) {
      this.host = host;
      this.file = file;
      if (username != null) {
         this.username = username;
      } else {
         this.username = System.getProperty("user.name");
      }
      if (port > 0) {
         this.port = port;
      } else {
         this.port = SSHClient.DEFAULT_PORT;
      }
   }

   @Override
   public void open() throws IOException {
      if (remoteStream != null) {
         close();
      }
      ssh = new SSHClient();
      ssh.loadKnownHosts();
      ssh.connect(host, port);
      ssh.authPublickey(username);
      sftp = ssh.newSFTPClient();
      final RemoteFile remoteFile = sftp.open(file);
      bufferedOutput = new PipedOutputStream();
      bufferedInput = new PipedInputStream(bufferedOutput, 1024 * 1024);
      remoteStream = remoteFile.getInputStream();
      preReader = new Thread() {
         @Override
         public void run() {
            byte[] buffer = new byte[1024 * 1024];
            int read;
            try {
               while (run && (read = remoteStream.read()) >= 0) {
                  bufferedOutput.write(buffer, 0, read);
               }
            } catch (IOException e) {
               thrown = new IOException(e);
            } finally {
               try {
                  try {
                     remoteStream.close();
                     bufferedOutput.close();
                  } finally {
                     try {
                        sftp.close();
                     } finally {
                        ssh.disconnect();
                     }
                  }
               } catch (IOException e) {
                  thrown = new IOException(e);
               } finally {
                  remoteStream = null;
                  bufferedOutput = null;
               }
            }
         }
      };
      preReader.setDaemon(true);
      preReader.setName(host + "-reader");
      thrown = null;
      preReader.start();
   }

   @Override
   public InputStream stream() throws IOException {
      return bufferedInput;
   }

   @Override
   public String shortName() {
      return new File(file).getName();
   }

   @Override
   public void close() throws IOException {
      run = false;
      try {
         preReader.join();
      } catch (InterruptedException e) {
      } finally {
         if (thrown != null) {
            throw thrown;
         }
      }
   }

   @Override
   public String toString() {
      return String.format("%s@%s:%d:%s", username, host, port, file);
   }
}

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

package org.mft.logic;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.mft.persistence.Input;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SFTPInput implements Input {
   private final String host;
   private final String file;
   private final int port;
   private final String username;

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
   public int peek(byte[] bytes) throws IOException {
      SSHClient ssh = new SSHClient();
      try {
         ssh.loadKnownHosts();
         ssh.connect(host, port);
         ssh.authPublickey(username);
         SFTPClient sftp = ssh.newSFTPClient();
         try {
            final RemoteFile remoteFile = sftp.open(file);
            return remoteFile.read(0, bytes, 0, bytes.length);
         } finally {
            sftp.close();
         }
      } finally {
         ssh.close();
      }
   }

   @Override
   public InputStream stream() throws IOException {
      SSHClient ssh = new SSHClient();
      ssh.loadKnownHosts();
      ssh.connect(host, port);
      ssh.authPublickey(username);
      SFTPClient sftp = ssh.newSFTPClient();
      RemoteFile remoteFile = sftp.open(file);
      PipedOutputStream bufferedOutput = new PipedOutputStream();
      PipedInputStream bufferedInput = new PipedInputStream(bufferedOutput, 1024 * 1024);
      RemoteFile.RemoteFileInputStream remoteStream = remoteFile.getInputStream();
      AtomicBoolean run = new AtomicBoolean();
      AtomicReference<IOException> thrown = new AtomicReference<>();
      Thread preReader = new Thread() {
         @Override
         public void run() {
            byte[] buffer = new byte[1024 * 1024];
            int read;
            try {
               while (run.get() && (read = remoteStream.read()) >= 0) {
                  bufferedOutput.write(buffer, 0, read);
               }
            } catch (IOException e) {
               thrown.set(new IOException(e));
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
                  thrown.set(new IOException(e));
               }
            }
         }
      };
      preReader.setDaemon(true);
      preReader.setName(host + "-reader");
      preReader.start();
      return new FilterInputStream(bufferedInput) {
         @Override
         public void close() throws IOException {
            run.set(false);
            if (thrown.get() != null) {
               throw new IOException("Pre-reader thread has thrown an exception", thrown.get());
            }
            super.close();
         }
      };
   }

   @Override
   public String name() {
      return new File(file).getName();
   }

   @Override
   public String toString() {
      return String.format("%s@%s:%d:%s", username, host, port, file);
   }
}

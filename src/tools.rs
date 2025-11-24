/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-21
 *  @Filename: tools.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use tokio::sync::mpsc;
pub(crate) struct BasicMPSCChannel {
    reader: mpsc::Receiver<bytes::Bytes>,
    writer: mpsc::Sender<bytes::Bytes>,
}

impl BasicMPSCChannel {
    /// Creates a new BasicMPSCChannel with the specified buffer size.
    pub(crate) fn new(buffer_size: usize) -> Self {
        let (writer, reader) = mpsc::channel(buffer_size);
        BasicMPSCChannel { reader, writer }
    }

    /// Returns a clone of the writer.
    pub(crate) fn get_writer(&self) -> mpsc::Sender<bytes::Bytes> {
        self.writer.clone()
    }

    /// Returns the reader.
    pub(crate) fn get_reader(self) -> mpsc::Receiver<bytes::Bytes> {
        self.reader
    }
}

part of postgres;

class _MessageFrame {
  static const int HeaderByteSize = 5;
  static Map<int, Function> _messageTypeMap = {
    49: () => new _ParseCompleteMessage(),
    50: () => new _BindCompleteMessage(),
    67: () => new _CommandCompleteMessage(),
    68: () => new _DataRowMessage(),
    69: () => new _ErrorResponseMessage(),
    75: () => new _BackendKeyMessage(),
    82: () => new _AuthenticationMessage(),
    83: () => new _ParameterStatusMessage(),
    84: () => new _RowDescriptionMessage(),
    90: () => new _ReadyForQueryMessage(),
    110: () => new _NoDataMessage(),
    116: () => new _ParameterDescriptionMessage()
  };

  BytesBuilder inputBuffer = new BytesBuilder(copy: false);
  bool get hasReadHeader => type != null;
  int type;
  int expectedLength;

  bool get isComplete => data != null;
  Uint8List data;

  int addBytes(Uint8List bytes) {
    inputBuffer.add(new Uint8List.view(bytes.buffer, bytes.offsetInBytes));

    // If we don't yet have a full header, inform that we consumed all of
    // the bytes and wait for the next packet.
    if (!hasReadHeader && inputBuffer.length < HeaderByteSize) {
      return bytes.length;
    }

    var combinedBytes = inputBuffer.takeBytes();
    var offsetIntoIncomingBytes = 0;
    var byteBufferLengthRemaining = combinedBytes.length;
    if (!hasReadHeader) {
      var headerBuffer = new Uint8List(5)
        ..setRange(0, HeaderByteSize, combinedBytes);

      var bufReader = new ByteData.view(headerBuffer.buffer);
      type = bufReader.getUint8(0);
      expectedLength = bufReader.getUint32(1) - 4;

      offsetIntoIncomingBytes += HeaderByteSize;
      byteBufferLengthRemaining -= HeaderByteSize;
    }

    // If we don't have enough to fully construct this message,
    // add the remaining bytes to the buffer. We've already set the header,
    // so we can discard those bytes.
    if (byteBufferLengthRemaining < expectedLength) {
      inputBuffer.add(combinedBytes.sublist(offsetIntoIncomingBytes, combinedBytes.length));
      return bytes.length;
    }

    // We have exactly the right number of bytes, so indicate we consumed all
    // of the new bytes and take the data.
    if (byteBufferLengthRemaining == expectedLength) {
      data = combinedBytes.sublist(offsetIntoIncomingBytes, combinedBytes.length);
      return bytes.length;
    }

    // If we got all the data we need, but still have more bytes,
    // we can take the data and let the caller know we didn't consume
    // all of the bytes.
    data = combinedBytes.sublist(offsetIntoIncomingBytes, expectedLength + offsetIntoIncomingBytes);
    offsetIntoIncomingBytes += expectedLength;
    byteBufferLengthRemaining -= expectedLength;
    inputBuffer.add(combinedBytes.sublist(offsetIntoIncomingBytes, combinedBytes.length));

    return bytes.length - byteBufferLengthRemaining;
  }

  _ServerMessage get message {
    var msgMaker = _messageTypeMap[type];
    if (msgMaker == null) {
      msgMaker = () {
        var msg = new _UnknownMessage()..code = type;
        return msg;
      };
    }

    _ServerMessage msg = msgMaker();

    msg.readBytes(data);

    return msg;
  }
}

class _MessageFramer {
  _MessageFrame messageInProgress = new _MessageFrame();
  List<_MessageFrame> messageQueue = [];

  void addBytes(Uint8List bytes) {
    var offsetIntoBytesRead = 0;

    do {
      offsetIntoBytesRead += messageInProgress
          .addBytes(new Uint8List.view(bytes.buffer, offsetIntoBytesRead));

      if (messageInProgress.isComplete) {
        messageQueue.add(messageInProgress);
        messageInProgress = new _MessageFrame();
      }
    } while (offsetIntoBytesRead != bytes.length);
  }

  bool get hasMessage => messageQueue.isNotEmpty;

  _MessageFrame popMessage() {
    return messageQueue.removeAt(0);
  }
}

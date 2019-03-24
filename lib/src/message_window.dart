import 'dart:collection';
import 'dart:typed_data';

import 'package:buffer/buffer.dart';

import 'server_messages.dart';

const int _headerByteSize = 5;
final _emptyData = Uint8List(0);

typedef ServerMessage _ServerMessageFn();

Map<int, _ServerMessageFn> _messageTypeMap = {
  49: () => ParseCompleteMessage(),
  50: () => BindCompleteMessage(),
  65: () => NotificationResponseMessage(),
  67: () => CommandCompleteMessage(),
  68: () => DataRowMessage(),
  69: () => ErrorResponseMessage(),
  75: () => BackendKeyMessage(),
  82: () => AuthenticationMessage(),
  83: () => ParameterStatusMessage(),
  84: () => RowDescriptionMessage(),
  90: () => ReadyForQueryMessage(),
  110: () => NoDataMessage(),
  116: () => ParameterDescriptionMessage()
};

class MessageFramer {
  final _reader = ByteDataReader();
  final messageQueue = Queue<ServerMessage>();

  int _type;
  int _expectedLength;

  bool get _hasReadHeader => _type != null;
  bool get _canReadHeader => _reader.remainingLength >= _headerByteSize;

  bool get _isComplete =>
      _expectedLength == 0 || _expectedLength <= _reader.remainingLength;

  void addBytes(Uint8List bytes) {
    _reader.add(bytes);

    bool evaluateNextMessage = true;
    while (evaluateNextMessage) {
      evaluateNextMessage = false;

      if (!_hasReadHeader && _canReadHeader) {
        _type = _reader.readUint8();
        _expectedLength = _reader.readUint32() - 4;
      }

      if (_hasReadHeader && _isComplete) {
        final data =
            _expectedLength == 0 ? _emptyData : _reader.read(_expectedLength);
        final msgMaker = _messageTypeMap[_type];
        final msg =
            msgMaker == null ? (UnknownMessage()..code = _type) : msgMaker();
        msg.readBytes(data);
        messageQueue.add(msg);
        _type = null;
        _expectedLength = null;
        evaluateNextMessage = true;
      }
    }
  }

  bool get hasMessage => messageQueue.isNotEmpty;

  ServerMessage popMessage() {
    return messageQueue.removeFirst();
  }
}

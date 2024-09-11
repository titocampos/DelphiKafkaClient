unit Kafka.Types;

interface

uses
  SysUtils;

type
  PNativeUInt = ^NativeUInt;
  mode_t = LongWord;

  TKafkaErrorArray = Array[0 .. 512] of AnsiChar;

  TKafkaLogType = (
    kltLog,
    kltError,
    kltProducer,
    kltConsumer,
    kltDebug
  );

const
  KafkaLogTypeDescriptions: Array[TKafkaLogType] of String = (
   'Log',
   'Error',
   'Producer',
   'Consumer',
   'Debug'
  );

implementation

end.

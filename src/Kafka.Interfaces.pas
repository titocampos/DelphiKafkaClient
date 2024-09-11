unit Kafka.Interfaces;

interface

uses
  CRM.Types, SysUtils,  Kafka.Lib;

type
  IKafkaInterface = interface
    ['{B2F30971-1971-45D7-8694-7C946E5D91E8}']
  end;

  IKafkaProducer = interface(IKafkaInterface)
    ['{DCED73C8-0F12-4E82-876C-ACF90940D2C2}']
    function GetProducedCount: Integer;
    function GetKafkaHandle: prd_kafka_t;
    function Produce(const Topic: String; const Payload: Pointer; const PayloadLength: NativeUInt; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    function Produce(const Topic: String; const Payloads: TPointerArray; const PayloadLengths: TIntegerArray; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    function Produce(const Topic: String; const Payload: String; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    function Produce(const Topic: String; const Payloads: TStringArray; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    property KafkaHandle: prd_kafka_t read GetKafkaHandle;
    property ProducedCount: Integer read GetProducedCount;
  end;

  IKafkaConsumer = interface(IKafkaInterface)
    ['{7C124CC8-B64D-45EE-B3D4-99DA5653349C}']
    function GetConsumedCount: Integer;
    property ConsumedCount: Integer read GetConsumedCount;
  end;

implementation

end.

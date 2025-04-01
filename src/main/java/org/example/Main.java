package org.example;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        String brokerHost = "localhost";
        int brokerPort = 9092;


        try (Socket socket = new Socket(brokerHost, brokerPort)) {
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            byte[] createTopicRequest = createCreateTopicRequest();

            outputStream.write(createTopicRequest);
            outputStream.flush();

            parseCreateTopicsResponse(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void parseCreateTopicsResponse(InputStream inputStream) throws Exception {
        int responseSize = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
        byte[] responseBody = inputStream.readNBytes(responseSize);
        ByteBuffer buffer = ByteBuffer.wrap(responseBody);

        // Read and print correlation id (INT32)
        int correlationId = buffer.getInt();
        System.out.println("correlationid: " + correlationId);

        // tag buffer header
        int taggesFieldsHeader = readVarInt(buffer);
        System.out.println("number of tagges fields header: " + taggesFieldsHeader);


        int throttleTimeMs = buffer.getInt();
        System.out.println("throttleTimeMs: " + throttleTimeMs);

        // Read topics array size (compact array, varint encoded)
        int topicsCount = readVarInt(buffer) - 1;
        System.out.println("Number of Topics: " + topicsCount);

        for (int i = 0; i < topicsCount; i++) {
            // Read name (COMPACT_STRING)
            String name = readCompactString(buffer);
            System.out.println("Topic Name: " + name);

            // Read topic_id (UUID)
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            UUID topicId = new UUID(mostSigBits, leastSigBits);
            System.out.println("Topic ID: " + topicId);

            // Read error_code (INT16)
            short errorCode = buffer.getShort();
            System.out.println("Error Code: " + errorCode);

            // Read error_message (COMPACT_NULLABLE_STRING)
            String errorMessage = readCompactNullableString(buffer);
            System.out.println("Error Message: " + (errorMessage == null ? "null" : errorMessage));

            // Read num_partitions (INT32)
            int numPartitions = buffer.getInt();
            System.out.println("Num Partitions: " + numPartitions);

            // Read replication_factor (INT16)
            short replicationFactor = buffer.getShort();
            System.out.println("Replication Factor: " + replicationFactor);

            // Read configs array size (compact array, varint encoded)
            int configsCount = readVarInt(buffer) - 1;
            System.out.println("Configs Count: " + configsCount);

            for (int j = 0; j < configsCount; j++) {
                // Read config name (COMPACT_STRING)
                String configName = readCompactString(buffer);
                System.out.println("  Config Name: " + configName);

                // Read config value (COMPACT_NULLABLE_STRING)
                String configValue = readCompactNullableString(buffer);
                System.out.println("  Config Value: " + (configValue == null ? "null" : configValue));

                // Read read_only (BOOLEAN)
                boolean readOnly = buffer.get() != 0;
                System.out.println("  Read Only: " + readOnly);

                // Read config_source (INT8)
                byte configSource = buffer.get();
                System.out.println("  Config Source: " + configSource);

                // Read is_sensitive (BOOLEAN)
                boolean isSensitive = buffer.get() != 0;
                System.out.println("  Is Sensitive: " + isSensitive);

                // tag buffer header
                int taggesFieldsConfig = readVarInt(buffer);
                System.out.println("number of tagges fields config: " + taggesFieldsConfig);
            }
            // tag buffer header
            int taggesFieldsTopic = readVarInt(buffer);
            System.out.println("number of tagges fields topic: " + taggesFieldsTopic);
        }
        // tag buffer header
        int taggesFieldsResponse = readVarInt(buffer);
        System.out.println("number of tagges fields response: " + taggesFieldsResponse);
    }

    private static String readCompactString(ByteBuffer buffer) {
        int length = readVarInt(buffer) - 1;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readCompactNullableString(ByteBuffer buffer) {
        int length = readVarInt(buffer) - 1;
        if (length == -1) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readVarInt(ByteBuffer buffer) {
        int value = 0;
        int position = 0;
        byte currentByte;
        do {
            currentByte = buffer.get();
            value |= (currentByte & 0x7F) << position;
            position += 7;
        } while ((currentByte & 0x80) != 0);
        return value;
    }

    private static byte[] createHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
        ByteBuffer headerBuffer = ByteBuffer.allocate(2 + 2 + 4 + 2 + clientId.length() + 1);
        headerBuffer.putShort(apiKey);
        headerBuffer.putShort(apiVersion);
        headerBuffer.putInt(correlationId);
        headerBuffer.putShort((short) clientId.length());
        headerBuffer.put(clientId.getBytes());
        writeUnsignedVarint(0, headerBuffer);
        return headerBuffer.array();
    }


    private static byte[] createCreateTopicRequest() throws IOException {
        short apiKey = 19;  // Create Topic API Key
        short apiVersion = 7;  // Version
        int correlationId = 1;
        String clientId = "client-id";

        byte[] header = createHeader(apiKey, apiVersion, correlationId, clientId);
        byte[] payload = createPayloadCreateTopic();

        ByteBuffer buffer = ByteBuffer.allocate(4 + header.length + payload.length);
        buffer.putInt(header.length + payload.length); // Total size excluding this int
        buffer.put(header);
        buffer.put(payload);

        return buffer.array();
    }


// CreateTopics Request (Version: 7) => [topics] timeout_ms validate_only _tagged_fields
//    topics => name num_partitions replication_factor [assignments] [configs] _tagged_fields
//       name => COMPACT_STRING
//       num_partitions => INT32
//       replication_factor => INT16
//       assignments => partition_index [broker_ids] _tagged_fields
//          partition_index => INT32
//          broker_ids => INT32
//       configs => name value _tagged_fields
//          name => COMPACT_STRING
//          value => COMPACT_NULLABLE_STRING
//    timeout_ms => INT32
//    validate_only => BOOLEAN
    private static byte[] createPayloadCreateTopic() {
        String topicName = "kjdsklsjdflkjds";
        int numberOfTopics = 1;
        int numOfPartitions = 1;
        short replicationFactor = 1;
        int numberOfAssignments = 0;
        int numberOfConfigs = 0;
        int numberOfTaggedFields = 0;
        int timeoutMs = 5000;
        byte validateOnly = 0;

        ByteBuffer payloadRequest = ByteBuffer.allocate(512);
        writeUnsignedVarint(numberOfTopics + 1, payloadRequest);
        writeCompactString(topicName, payloadRequest);
        payloadRequest.putInt(numOfPartitions);
        payloadRequest.putShort(replicationFactor);
        writeUnsignedVarint(numberOfAssignments + 1, payloadRequest);
        writeUnsignedVarint(numberOfConfigs + 1, payloadRequest);
        writeUnsignedVarint(numberOfTaggedFields, payloadRequest);
        payloadRequest.putInt(timeoutMs);
        payloadRequest.put(validateOnly);
        writeUnsignedVarint(numberOfTaggedFields, payloadRequest);
        return trimByteBuffer(payloadRequest).array();
    }


    public static ByteBuffer trimByteBuffer(ByteBuffer buffer) {
        buffer.flip();
        ByteBuffer trimmedBuffer = ByteBuffer.allocate(buffer.remaining());
        trimmedBuffer.put(buffer);
        trimmedBuffer.flip();
        return trimmedBuffer;
    }








    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    private static void writeCompactString(String value, ByteBuffer buffer) {
        if (value == null) {
            throw new IllegalArgumentException("Compact strings cannot be null");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(bytes.length + 1, buffer);
        buffer.put(bytes);
    }



}
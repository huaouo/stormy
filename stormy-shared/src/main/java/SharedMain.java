import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.huaouo.stormy.stream.DynamicSchema;
import com.huaouo.stormy.stream.FieldType;
import com.huaouo.stormy.stream.MessageDefinition;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.RedisCommand;

import java.io.ByteArrayInputStream;
import java.util.concurrent.TimeUnit;

public class SharedMain {
    public static void main(String[] args) throws Exception {
//        System.out.println(ByteString.readFrom(new ByteArrayInputStream(new byte[0])));
//        MessageDefinition msgDef = MessageDefinition.newBuilder("TupleData")
//                .addField(FieldType.INT, "id")
//                .addField(FieldType.STRING, "email")
//                .build();
//
//        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
//        schemaBuilder.addMessageDefinition(msgDef);
//        DynamicSchema schema = schemaBuilder.build();
//
//        DynamicMessage.Builder msgBuilder = schema.newMessageBuilder("TupleData");
//        Descriptor msgDesc = msgBuilder.getDescriptorForType();
//        DynamicMessage msg = msgBuilder.setField(msgDesc.findFieldByName("id"), 1)
//                .setField(msgDesc.findFieldByName("email"), "huaouo@live.com")
//                .build();
//
//        byte[] msgBytes = msg.toByteArray();
//        byte[] schemaBytes = schema.toByteArray();
//
//        DynamicSchema parsedSchema = DynamicSchema.parseFrom(schemaBytes);
//        DynamicMessage.Builder parsedMessageBuilder = parsedSchema.newMessageBuilder("TupleData");
//        Descriptor parsedMsgDesc = parsedMessageBuilder.getDescriptorForType();
//        DynamicMessage parsedMessage = parsedMessageBuilder.mergeFrom(msgBytes).build();
//        System.out.println(parsedMessage.getField(parsedMsgDesc.findFieldByName("email")));

        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        syncCommands.set("key", "Hello, Redis!");
        System.out.println(syncCommands.get("key"));
        connection.close();
        redisClient.shutdown();

//        RedisClient redisClient = RedisClient.create("redis://localhost");
//        StatefulRedisConnection<String, String> connection = redisClient.connect();
//        RedisAsyncCommands<String, String> asyncCommands = connection.async();
//        RedisFuture<String> future = asyncCommands.get("key");
//        future.thenAccept(System.out::println);
//        System.out.println("Hello, World!");
//        future.await(100, TimeUnit.SECONDS);
//        int sum = 0;
//        for (int i = 0; i < 100000000; i++) {
//            sum += i;
//        }
    }
}

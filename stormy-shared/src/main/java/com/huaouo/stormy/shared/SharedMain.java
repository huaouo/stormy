// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.shared;

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

//        ZooKeeper zk = new ZooKeeper();
//        zk.create()
    }
}

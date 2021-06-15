package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import akka.japi.tuple.Tuple4;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import akka.stream.*;
import akka.stream.javadsl.*;
import org.apache.commons.lang3.tuple.Triple;
import scala.Tuple2;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	public static final int MESSAGE_SIZE = 100000;

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class SendMessage implements Serializable {
		private static final long serialVersionUID = 4257807743872319842L;
		private ActorRef senderProxy;
		private ActorRef sender;
		private ActorRef receiver;
		private String id;
		private byte[] data;
		private long offset;
		private boolean isFinalPackage = false;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class AckMessage implements Serializable {
		private static final long serialVersionUID = 4257807743872319855L;
		private long bytesReceived = 0L;
		private ActorRef receiver;

	}

	/////////////////
	// Actor State //
	/////////////////

	HashMap<ActorRef,Queue<Tuple4<ActorRef, ActorRef, String, byte[]>>> messageQueues = new HashMap<>();
	HashMap<Tuple2<ActorRef, String>, ArrayList<SendMessage>> receiverBuffer = new HashMap<>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(SendMessage.class, this::handle)
				.match(AckMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.

		byte[] bytes = KryoPoolSingleton.get().toBytesWithClass(message);
		this.messageQueues.computeIfAbsent(receiver, k -> new LinkedList<>());
		String messageId =  UUID.randomUUID().toString();
		this.messageQueues.get(receiver).add(new Tuple4(sender, receiver, messageId, bytes));
		sendPackage(0, receiver);

		//receiverProxy.tell(new BytesMessage<>(message, sender, receiver), this.self());
	}

	private void sendPackage(long offset, ActorRef receiver){
		Tuple4<ActorRef, ActorRef, String, byte[]> data = messageQueues.get(receiver).peek();
		boolean isFinal = data.t4().length - offset <= MESSAGE_SIZE;
		byte[] bytes = Arrays.copyOfRange(data.t4(), (int)offset, (int)Math.min(data.t4().length, offset+MESSAGE_SIZE));
		SendMessage msg = new SendMessage(this.self(), data.t1(), data.t2(), data.t3(), bytes, offset, isFinal);
		ActorSelection receiverProxy = this.context().actorSelection(data.t2().path().child(DEFAULT_NAME));
		if(isFinal){
			messageQueues.remove(receiver);
		}
		//System.out.println("Send message" + msg.offset + " from " + msg.sender + "with id " + msg.id);
		receiverProxy.tell(msg, this.self());
	}

	private void handle(AckMessage message) {
		if(messageQueues.containsKey(message.receiver)){
			sendPackage(message.bytesReceived, message.receiver);
		}
	}

	private void handle(SendMessage message){
		//System.out.println("Received message" + message.offset + " from " + message.sender + "with id " + message.id);
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		Tuple2 key = new Tuple2(this.sender(), message.id);
		receiverBuffer.computeIfAbsent(key, s -> new ArrayList()).add(message);
		if(message.isFinalPackage()){
			ArrayList<Byte> bytes = new ArrayList<Byte>();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			for(SendMessage msg : receiverBuffer.get(key)){
				try {
					outputStream.write(msg.data);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			message.receiver.tell(KryoPoolSingleton.get().fromBytes(outputStream.toByteArray()), message.sender);
		}
		message.senderProxy.tell(new AckMessage(message.offset + message.data.length, message.receiver), this.self());
	}
}

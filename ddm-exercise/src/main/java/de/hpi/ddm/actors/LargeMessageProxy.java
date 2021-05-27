package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import akka.stream.*;
import akka.stream.javadsl.*;
import org.apache.commons.lang3.tuple.Triple;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	public static final int MESSAGE_SIZE = 100;
	
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
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class SendMessage implements Serializable {
		private static final long serialVersionUID = 4257807743872319842L;
		private ActorRef senderProxy;
		private ActorRef sender;
		private ActorRef receiver;
		private byte[] data;
		private long offset;
		private boolean isFinalPackage = false;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class AckMessage implements Serializable {
		private static final long serialVersionUID = 4257807743872319842L;
		private long bytesReceived = 0L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	Queue<Triple<ActorRef, ActorRef, byte[]>> messageQueue = new LinkedList<>();
	HashMap<ActorRef, ArrayList<SendMessage>> receiverBuffer = new HashMap<>();
	
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
				.match(BytesMessage.class, this::handle)
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
		this.messageQueue.add(Triple.of(sender, receiver, bytes));
		sendPackage(0);

		//receiverProxy.tell(new BytesMessage<>(message, sender, receiver), this.self());
	}

	private void sendPackage(long offset){
		Triple<ActorRef, ActorRef, byte[]> data = messageQueue.peek();
		boolean isFinal = data.getRight().length - offset <= MESSAGE_SIZE;
		byte[] bytes = Arrays.copyOfRange(data.getRight(), (int)offset, (int)Math.min(data.getRight().length, offset+MESSAGE_SIZE));
		SendMessage msg = new SendMessage(this.self(), data.getLeft(), data.getMiddle(), bytes, offset, isFinal);
		ActorSelection receiverProxy = this.context().actorSelection(data.getMiddle().path().child(DEFAULT_NAME));
		if(isFinal){
			messageQueue.poll();
		}
		receiverProxy.tell(msg, this.self());
	}

	private void handle(BytesMessage<?> message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}

	private void handle(AckMessage message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		if(!messageQueue.isEmpty()){
			sendPackage(message.bytesReceived);
		}
	}

	private void handle(SendMessage message){
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		receiverBuffer.computeIfAbsent(message.senderProxy, s -> new ArrayList()).add(message);
		if(message.isFinalPackage()){
			ArrayList<Byte> bytes = new ArrayList<Byte>();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			for(SendMessage msg : receiverBuffer.get(message.senderProxy)){
				try {
					outputStream.write(msg.data);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			message.receiver.tell(KryoPoolSingleton.get().fromBytes(outputStream.toByteArray()), message.sender);
		}
		message.senderProxy.tell(new AckMessage(message.offset + message.data.length), this.self());
	}
}

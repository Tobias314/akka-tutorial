package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import scala.Tuple2;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 834304098948609598L;
		private BloomFilter welcomeData;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PwWithConstraints implements Serializable {
		String userName;
		String pwHash;
		TreeSet<Character> notIncludedCharacters;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ReducedProblemMessage implements Serializable {
		ArrayList<PwWithConstraints> contrainedPws ;
		int pwLength;
		String pwCharacters;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(Master.HintProblemMessage.class, this::handle)
				.match(ReducedProblemMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
	}

	private void handle(ReducedProblemMessage reducedProblem){
		Master.ResultMessage resultMsg = new Master.ResultMessage(new ArrayList<>());
		String allChars = reducedProblem.pwCharacters;
		for(PwWithConstraints constrainedPw : reducedProblem.contrainedPws){
			//System.out.println("handling reduced Problem");
			ArrayList<Character> testSet = new ArrayList();
			for(int i = 0; i<allChars.length(); i++){
				if(!constrainedPw.notIncludedCharacters.contains(allChars.charAt(i))){
					testSet.add(allChars.charAt(i));
				}
			}
			String result = testAllKLengthRec(testSet, "", testSet.size(), reducedProblem.pwLength, constrainedPw.pwHash);
			if(result != null){
				resultMsg.data.add(new Tuple2(constrainedPw.userName, result));
			}
			//System.out.println("finished handling reduced Problem");
		}
		this.sender().tell(resultMsg, this.self());
	}

	static void printAllKLength(ArrayList<Character> set, int k, String toTest)
	{
		int n = set.size();
		testAllKLengthRec(set, "", set.size(), k, toTest);
	}
	static String testAllKLengthRec(ArrayList<Character> set, String prefix, int n, int k, String toTest)
	{
		if (k == 0){
			if(hash(prefix).equals(toTest)){
				return prefix;
			}else{
				return null;
			}
		}
		for (int i = 0; i < n; ++i){
			String newPrefix = prefix + set.get(i);
			String result = testAllKLengthRec(set, newPrefix, n, k - 1, toTest);
			if(result != null){
				return result;
			}
		}
		return null;
	}

	private void handle(Master.HintProblemMessage problem){
		ArrayList<PwWithConstraints> pwWithConstraints = new ArrayList<>();
		HashMap <String, ArrayList<Integer>> hintMap = new HashMap<>();
		for(int i = 0; i<problem.data.size(); i++){
			TreeSet<Character> ts = new TreeSet<>();
			//for(int t=0; t<problem.pwChars.length(); t++){ts.add(problem.pwChars.charAt(t));}
			pwWithConstraints.add(new PwWithConstraints(problem.data.get(i).name, problem.data.get(i).pwHash, ts));
			for(String hint : problem.data.get(i).hintHashes){
				hintMap.computeIfAbsent(hint, s -> new ArrayList<>()).add(i);
			}
		}
		ReducedProblemMessage reducedProblem = new ReducedProblemMessage(pwWithConstraints, problem.pwLength, problem.pwChars);

		ArrayList<Character> chars = new ArrayList<Character>();
		for (char c : problem.pwChars.toCharArray()) {
			chars.add(c);
		}
		System.out.println("creating permutations with " + problem.excludedCharForHint +" char missing");

		heapPermutation(chars, chars.size(), chars.size(), reducedProblem, hintMap, problem.excludedCharForHint);

		System.out.println("finished creating permutations with " + problem.excludedCharForHint +" char missing");

		this.sender().tell(reducedProblem, this.self());
	}

	void heapPermutation(ArrayList<Character> characters, int size, int n, ReducedProblemMessage reducedProblem,
						 HashMap<String, ArrayList<Integer>> hintMap, char currentlyAvoiding)
	{
		// if size becomes 1 then prints the obtained
		// permutation
		if (size == 1) {
			String h = hash(stringFromCharacterArrayList(characters));
			ArrayList<Integer> tmp =hintMap.get(h);
			if(tmp != null){
				for(int i : tmp){
					//System.out.println("removing character " + currentlyAvoiding + "from possible characters");
					reducedProblem.contrainedPws.get(i).notIncludedCharacters.add(currentlyAvoiding);
				}
			}
			return;
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(characters, size - 1, n, reducedProblem, hintMap, currentlyAvoiding);

			// if size is odd, swap 0th i.e (first) and
			// (size-1)th i.e (last) element
			if (size % 2 == 1){
				Collections.swap(characters, 0, size - 1);
			}
			else {
				Collections.swap(characters, i, size - 1);
			}
		}
	}

	private String stringFromCharacterArrayList(ArrayList<Character> list){
		return list.stream().map(e->e.toString()).reduce((acc, e) -> acc  + e).get();
	}

	static private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));

			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	/*private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}*/
}
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
import de.hpi.ddm.stru.BloomFilter;
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
		private static final long serialVersionUID = 834304098948609513L;
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
				resultMsg.data.add(new Master.StringTuple2(constrainedPw.userName, result));
			}
			//System.out.println("finished handling reduced Problem");
		}
		this.sender().tell(resultMsg, this.self());
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
        //long pid = ProcessHandle.current().pid();
        //System.out.println("Process ID: " + pid);
		//System.out.println("Starting hint ecryption package");
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
		//System.out.println("creating permutations with " + problem.excludedCharForHint +" char missing");

		for(Master.StringTuple2 suffix : problem.suffixesToTry){
			String chars = problem.pwChars.replace(suffix._2, "");
			ArrayList<Character> chars2 = new ArrayList<>();
			for(char c : chars.toCharArray()){
				if(!suffix._1.contains(Character.toString(c))){
					chars2.add(c);
				}
			}
			//System.out.println("doing heapPermutation for: ..." + suffix._1 + ", excluded is" + suffix._2);
			heapPermutation(chars2, chars2.size(), suffix._2, suffix._1, hintMap, reducedProblem);
		}

		//System.out.println("finished creating permutations with " + problem.excludedCharForHint +" char missing");

		//System.out.println("finishing hint encryption package");
		this.sender().tell(reducedProblem, this.self());
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

	void heapPermutation(ArrayList<Character> characters, int size, String currentlyAvoiding,
						 String suffix, HashMap<String, ArrayList<Integer>> hintMap, ReducedProblemMessage reducedProblem)
	{
		// if size becomes 1 then prints the obtained
		// permutation
		if (size == 1) {
			String prefix = "";
			for(char c : characters){prefix += c;}
			String perm = prefix + suffix;
			//System.out.println("perm: " + perm + ", with missing " + currentlyAvoiding);
			String h = hash(perm);
			ArrayList<Integer> res = hintMap.get(h);
			if(res != null){
				//System.out.println("found Match");
				for(int i : res){
					reducedProblem.contrainedPws.get(i).notIncludedCharacters.add(currentlyAvoiding.charAt(0));
				}
			}
			return;
		}
		for (int i = 0; i < size; i++) {
			heapPermutation(characters, size - 1, currentlyAvoiding, suffix, hintMap, reducedProblem);

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
}
package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.stru.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Tuple2;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static final int REDUCED_PROBLEM_PACKAGE_SIZE = 100;

	public static final int SUFFIX_SIZE = 2;

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.currentWorker = 0;
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class PwData implements Serializable{
		long id;
		String name;
		String pwHash;
		ArrayList<String> hintHashes;
	}

	public static class StringTuple2 implements Serializable {
		String _1 = null;
		String _2 = null;

		StringTuple2(){}

		StringTuple2(String obj1, String obj2){
			_1 = obj1;
			_2 = obj2;
		}
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class HintProblemMessage implements Serializable {
		private static final long serialVersionUID = 3303085451659723997L;
		ArrayList<PwData> data;
		String pwChars;
		int pwLength;
		char excludedCharForHint;
		ArrayList<StringTuple2> suffixesToTry;
		public HintProblemMessage shallowCopy(){
			return new HintProblemMessage(data, pwChars, pwLength, excludedCharForHint, suffixesToTry);
		}
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class ResultMessage implements Serializable {
		private static final long serialVersionUID = 3303455451659723997L;
		ArrayList<StringTuple2> data;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final LinkedList<ActorRef> freeWorkers = new LinkedList<>();
	private int currentWorker;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;
	private ArrayList<Worker.ReducedProblemMessage> reducedProblemList = new ArrayList();
	private int resultCounter = 0;
	Worker.ReducedProblemMessage mergedReducedProblem;
	int runningHintWorkPackages = 0;
	boolean allHintWorkPackagesSent = false;
	ArrayList<StringTuple2> permutation_buffer = new ArrayList<>();
	final static int PERMUTATION_PACKAGE_SIZE = 10;
	private LinkedList<Object> workQueue = new LinkedList<>();


	HintProblemMessage problem = new HintProblemMessage();

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.ReducedProblemMessage.class, this::handle)
				.match(ResultMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		reducedProblemList = new ArrayList();
		problem.data = new ArrayList<>();
		resultCounter = 0;
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		// TODO: Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		if (message.getLines().isEmpty()) {
			ArrayList<Worker.PwWithConstraints> constrainedPws = new ArrayList<>();
			for(int i = 0; i<problem.data.size(); i++){
				TreeSet<Character> ts = new TreeSet<>();
				constrainedPws.add(new Worker.PwWithConstraints(problem.data.get(i).name, problem.data.get(i).pwHash, ts));
			}
			mergedReducedProblem = new Worker.ReducedProblemMessage(constrainedPws, problem.pwLength, problem.pwChars);
			runningHintWorkPackages = 0;
			allHintWorkPackagesSent = false;
			distributedHintProcessing();
		}else{
			for(String[] items : message.getLines()){
				PwData pw = new Master.PwData();
				pw.id = Long.valueOf(items[0]);
				pw.name = items[1];
				pw.pwHash = items[4];
				pw.hintHashes = new ArrayList<>();
				for(int i = 5; i<items.length; i++){
					pw.hintHashes.add(items[i]);
				}
				problem.data.add(pw);
			}
			problem.pwChars =  message.getLines().get(0)[2];
			problem.pwLength = Integer.valueOf(message.getLines().get(0)[3]);
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}

		
		// TODO: Process the lines with the help of the worker actors
		//for (String[] line : message.getLines())
			//this.log().error("Need help processing: {}", Arrays.toString(line));
		
		// TODO: Send (partial) results to the Collector
		//this.collector.tell(new Collector.CollectMessage("If I had results, this would be one."), this.self());
		
		// TODO: Fetch further lines from the Reader
	}

	private void addFreeWorker(ActorRef worker){
		//System.out.println("add worker");
		freeWorkers.push(worker);
		tryAssignWork();
	}

	private void tryAssignWork(){
		while(!freeWorkers.isEmpty() && !workQueue.isEmpty()){
			ActorRef w = freeWorkers.pop();
			w.tell(workQueue.pop(), this.self());
		}
	}

	private void distributedHintProcessing(){
		for(int i = 0; i<problem.pwChars.length(); i++){
			//ArrayList<Character> reducedChars = new ArrayList<>();
			String reducedChars = "";
			char excluded = ' ';
			for(int j = 0; j<problem.pwChars.length(); j++){
				if(i!=j){
					reducedChars += problem.pwChars.charAt(j);
				}else{
					excluded = problem.pwChars.charAt(j);
				}
			}
			//heapPermutation(reducedChars, reducedChars.size(), reducedChars.size(), excluded);
			suffixPermutation(reducedChars, "", excluded);
		}
		scheduleHintWorkPackage();
		allHintWorkPackagesSent = true;
	}

	private void scheduleWork(Object msg){
		//workers.get(currentWorker).tell(msg, this.self());
		//currentWorker= (currentWorker + 1) % workers.size();
		workQueue.push(msg);
		//System.out.println("add work");
		tryAssignWork();
	}

	protected void handle(Worker.ReducedProblemMessage reducedProblem){
		addFreeWorker(this.sender());
		for(int i = 0; i < reducedProblem.contrainedPws.size(); i++){
			mergedReducedProblem.contrainedPws.get(i).notIncludedCharacters.addAll(reducedProblem.contrainedPws.get(i).notIncludedCharacters);
		}
		runningHintWorkPackages -= 1;

		if(runningHintWorkPackages == 0 && allHintWorkPackagesSent) {
			Worker.ReducedProblemMessage reducedProblemPackage = new Worker.ReducedProblemMessage(new ArrayList<>(), reducedProblem.pwLength, problem.pwChars);
			int count = 0;
			for (Worker.PwWithConstraints constrainedPw : mergedReducedProblem.contrainedPws) {
				reducedProblemPackage.contrainedPws.add(constrainedPw);
				count++;
				if (count >= REDUCED_PROBLEM_PACKAGE_SIZE) {
					scheduleWork(reducedProblemPackage);
					reducedProblemPackage = new Worker.ReducedProblemMessage(new ArrayList<>(), reducedProblem.pwLength, problem.pwChars);
				}
			}
			if(reducedProblemPackage.contrainedPws.size() > 0){
				scheduleWork(reducedProblemPackage);
			}
		}
	}

	protected void handle(ResultMessage result){
		this.addFreeWorker(this.sender());
		for(StringTuple2 namePw : result.data){
			this.collector.tell(new Collector.CollectMessage("User with name " + namePw._1 + " has Pw " + namePw._2), this.self());
		}
		this.resultCounter += result.data.size();
		if(resultCounter >= problem.data.size()){
			terminate();
		}
	}
	
	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.addFreeWorker(this.sender());
		this.log().info("Registered {}", this.sender());
		
		this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());
		
		// TODO: Assign some work to registering workers. Note that the processing of the global task might have already started.
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	private void scheduleHintWorkPackage(){
		HintProblemMessage subProblem = problem.shallowCopy();
		subProblem.suffixesToTry = permutation_buffer;
		runningHintWorkPackages += 1;
		scheduleWork(subProblem);
		permutation_buffer = new ArrayList<>();
	}

	private void addSuffix(String suffix, char missingCharacter){
		permutation_buffer.add(new StringTuple2(suffix, Character.toString(missingCharacter)));
		if(permutation_buffer.size()>= PERMUTATION_PACKAGE_SIZE){
			scheduleHintWorkPackage();
		}
	}

	void heapPermutation(ArrayList<Character> characters, int size, int n, char currentlyAvoiding)
	{
		// if size becomes 1 then prints the obtained
		// permutation
		if (size == SUFFIX_SIZE) {
			String suffix = "";
			for(int i = characters.size() - SUFFIX_SIZE; i< characters.size(); i++){suffix += characters.get(i);}
			System.out.println(suffix + " with missing" + currentlyAvoiding);
			addSuffix(suffix, currentlyAvoiding);
			return;
		}
		for (int i = 0; i < size; i++) {
			heapPermutation(characters, size - 1, n, currentlyAvoiding);

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


	void suffixPermutation(String characters, String alreadyFound, char currentlyAvoiding)
	{
		// if size becomes 1 then prints the obtained
		// permutation
		if (alreadyFound.length() == SUFFIX_SIZE) {
			//System.out.println(alreadyFound + " with missing" + currentlyAvoiding);
			addSuffix(alreadyFound, currentlyAvoiding);
			return;
		}
		for (char c : characters.toCharArray()) {
			suffixPermutation(characters.replace(Character.toString(c), ""), alreadyFound + c, currentlyAvoiding);
		}
	}


}

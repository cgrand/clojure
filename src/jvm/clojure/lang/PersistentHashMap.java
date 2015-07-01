/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

package clojure.lang;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.RuntimeErrorException;

/*
 A persistent rendition of Phil Bagwell's Hash Array Mapped Trie

 Uses path copying for persistence
 HashCollision leaves vs. extended hashing
 Node polymorphism vs. conditionals
 No sub-tree pools or root-resizing
 Any errors are my own
 */

public class PersistentHashMap extends APersistentMap implements IEditableCollection, IObj, IMapIterable {

final int count;
final INode root;
final IPersistentMap _meta;

final public static PersistentHashMap EMPTY = new PersistentHashMap(0, null);
final private static Object NOT_FOUND = new Object();

static public IPersistentMap create(Map other){
	ITransientMap ret = EMPTY.asTransient();
	for(Object o : other.entrySet())
		{
		Map.Entry e = (Entry) o;
		ret = ret.assoc(e.getKey(), e.getValue());
		}
	return ret.persistent();
}

/*
 * @param init {key1,val1,key2,val2,...}
 */
public static PersistentHashMap create(Object... init){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < init.length; i += 2)
		{
		ret = ret.assoc(init[i], init[i + 1]);
		}
	return (PersistentHashMap) ret.persistent();
}

public static PersistentHashMap createWithCheck(Object... init){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < init.length; i += 2)
		{
		ret = ret.assoc(init[i], init[i + 1]);
		if(ret.count() != i/2 + 1)
			throw new IllegalArgumentException("Duplicate key: " + init[i]);
		}
	return (PersistentHashMap) ret.persistent();
}

static public PersistentHashMap create(ISeq items){
	ITransientMap ret = EMPTY.asTransient();
	for(; items != null; items = items.next().next())
		{
		if(items.next() == null)
			throw new IllegalArgumentException(String.format("No value supplied for key: %s", items.first()));
		ret = ret.assoc(items.first(), RT.second(items));
		}
	return (PersistentHashMap) ret.persistent();
}

static public PersistentHashMap createWithCheck(ISeq items){
	ITransientMap ret = EMPTY.asTransient();
	for(int i=0; items != null; items = items.next().next(), ++i)
		{
		if(items.next() == null)
			throw new IllegalArgumentException(String.format("No value supplied for key: %s", items.first()));
		ret = ret.assoc(items.first(), RT.second(items));
		if(ret.count() != i + 1)
			throw new IllegalArgumentException("Duplicate key: " + items.first());
		}
	return (PersistentHashMap) ret.persistent();
}

/*
 * @param init {key1,val1,key2,val2,...}
 */
public static PersistentHashMap create(IPersistentMap meta, Object... init){
	return create(init).withMeta(meta);
}

PersistentHashMap(int count, INode root){
	this.count = count;
	this.root = root;
	this._meta = null;
}

public PersistentHashMap(IPersistentMap meta, int count, INode root){
	this._meta = meta;
	this.count = count;
	this.root = root;
}

static int hash(Object k){
	return Util.hasheq(k);
}

public boolean containsKey(Object key){
	return (root != null) ? root.find(0, hash(key), key, NOT_FOUND) != NOT_FOUND : false;
}

public IMapEntry entryAt(Object key){
	return (root != null) ? root.find(0, hash(key), key) : null;
}

public IPersistentMap assoc(Object key, Object val){
	Box addedLeaf = new Box(null);
	INode newroot = (root == null ? BitmapIndexedNode.EMPTY : root) 
			.assoc(0, hash(key), key, val, addedLeaf);
	if(newroot == root)
		return this;
	return new PersistentHashMap(meta(), addedLeaf.val == null ? count : count + 1, newroot);
}

public Object valAt(Object key, Object notFound){
	return root != null ? root.find(0, hash(key), key, notFound) : notFound;
}

public Object valAt(Object key){
	return valAt(key, null);
}

public IPersistentMap assocEx(Object key, Object val) {
	if(containsKey(key))
		throw Util.runtimeException("Key already present");
	return assoc(key, val);
}

public IPersistentMap without(Object key){
	if(root == null)
		return this;
	INode newroot = root.without(0, hash(key), key);
	if(newroot == root)
		return this;
	return new PersistentHashMap(meta(), count - 1, newroot); 
}

static final Iterator EMPTY_ITER = new Iterator(){
    public boolean hasNext(){
        return false;
    }

    public Object next(){
        throw new NoSuchElementException();
    }

    public void remove(){
        throw new UnsupportedOperationException();
    }
};

private Iterator iterator(final IFn f){
    return (root == null) ? EMPTY_ITER : root.iterator(f);
}

public Iterator iterator(){
    return iterator(APersistentMap.MAKE_ENTRY);
}

public Iterator keyIterator(){
    return iterator(APersistentMap.MAKE_KEY);
}

public Iterator valIterator(){
    return iterator(APersistentMap.MAKE_VAL);
}

public Object kvreduce(IFn f, Object init){
	if(RT.isReduced(init))
		return ((IDeref)init).deref();
	if(root != null){
		init = root.kvreduce(f,init);
		if(RT.isReduced(init))
			return ((IDeref)init).deref();
		else
			return init;
	}
	return init;
}

public Object fold(long n, final IFn combinef, final IFn reducef,
                   IFn fjinvoke, final IFn fjtask, final IFn fjfork, final IFn fjjoin){
	//we are ignoring n for now
	Callable top = new Callable(){
		public Object call() throws Exception{
			Object ret = combinef.invoke();
			if(root != null)
				ret = combinef.invoke(ret, root.fold(combinef,reducef,fjtask,fjfork,fjjoin));
			return ret;
		}
	};
	return fjinvoke.invoke(top);
}

public int count(){
	return count;
}

public ISeq seq(){
	return root != null ? root.nodeSeq() : null; 
}

public IPersistentCollection empty(){
	return EMPTY.withMeta(meta());	
}

static int mask(int hash, int shift){
	//return ((hash << shift) >>> 27);// & 0x01f;
	return (hash >>> shift) & 0x01f;
}

public PersistentHashMap withMeta(IPersistentMap meta){
	return new PersistentHashMap(meta, count, root);
}

public TransientHashMap asTransient() {
	return new TransientHashMap(this);
}

public IPersistentMap meta(){
	return _meta;
}

static final class TransientHashMap extends ATransientMap {
	final AtomicReference<Thread> edit;
	volatile INode root;
	volatile int count;
	final Box leafFlag = new Box(null);


	TransientHashMap(PersistentHashMap m) {
		this(new AtomicReference<Thread>(Thread.currentThread()), m.root, m.count);
	}
	
	TransientHashMap(AtomicReference<Thread> edit, INode root, int count) {
		this.edit = edit;
		this.root = root; 
		this.count = count; 
	}

	ITransientMap doAssoc(Object key, Object val) {
//		Box leafFlag = new Box(null);
		leafFlag.val = null;
		INode n = (root == null ? BitmapIndexedNode.EMPTY : root)
			.assoc(edit, 0, hash(key), key, val, leafFlag);
		if (n != this.root)
			this.root = n; 
		if(leafFlag.val != null) this.count++;
		return this;
	}

	ITransientMap doWithout(Object key) {
		if (root == null) return this;
//		Box leafFlag = new Box(null);
		leafFlag.val = null;
		INode n = root.without(edit, 0, hash(key), key, leafFlag);
		if (n != root)
			this.root = n;
		if(leafFlag.val != null) this.count--;
		return this;
	}

	IPersistentMap doPersistent() {
		edit.set(null);
		return new PersistentHashMap(count, root);
	}

	Object doValAt(Object key, Object notFound) {
		if (root == null)
			return notFound;
		return root.find(0, hash(key), key, notFound);
	}

	int doCount() {
		return count;
	}
	
	void ensureEditable(){
		if(edit.get() == null)
			throw new IllegalAccessError("Transient used after persistent! call");
	}
}

static interface INode extends Serializable {
	INode assoc(int shift, int hash, Object key, Object val, Box addedLeaf);

	INode without(int shift, int hash, Object key);

	IMapEntry find(int shift, int hash, Object key);

	Object find(int shift, int hash, Object key, Object notFound);

	ISeq nodeSeq();

	INode assoc(AtomicReference<Thread> edit, int shift, int hash, Object key, Object val, Box addedLeaf);

	INode without(AtomicReference<Thread> edit, int shift, int hash, Object key, Box removedLeaf);

    public Object kvreduce(IFn f, Object init);

	Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin);

    // returns the result of (f [k v]) for each iterated element
    Iterator iterator(IFn f);
}

final static class BitmapIndexedNode implements INode{
	static final BitmapIndexedNode EMPTY = new BitmapIndexedNode(null, 0, 0, new Object[0]);
	
	int bitmap;
	int kvbitmap;
	Object[] array;
	final AtomicReference<Thread> edit;

	final int index(int bit){
		return Integer.bitCount(bitmap & (bit - 1)) + Integer.bitCount(kvbitmap & (bit - 1));
	}

	BitmapIndexedNode(AtomicReference<Thread> edit, int bitmap, int kvbitmap, Object[] array){
        this.bitmap = bitmap;
        this.kvbitmap = kvbitmap;
		this.array = array;
		this.edit = edit;
	}

	public INode assoc(int shift, int hash, Object key, Object val, Box addedLeaf){
		int bit = bitpos(hash, shift);
		int idx = index(bit);
		if((bitmap & bit) != 0) {
			if((kvbitmap & bit) == 0) {
	            INode node = (INode) array[idx];
				INode n = node.assoc(shift + 5, hash, key, val, addedLeaf);
				if(n == node)
					return this;
				return new BitmapIndexedNode(null, bitmap, kvbitmap, cloneAndSet(array, idx, n));
			} 
            Object k = array[idx];
            Object v = array[idx+1];
			if(Util.equiv(key, k)) {
				if(val == v)
					return this;
				return new BitmapIndexedNode(null, bitmap, kvbitmap, cloneAndSet(array, idx+1, val));
			} 
			addedLeaf.val = addedLeaf;
            int n = Integer.bitCount(bitmap) + Integer.bitCount(kvbitmap);
            Object[] newArray = new Object[n - 1];
            System.arraycopy(array, 0, newArray, 0, idx);
            newArray[idx] = createNode(shift + 5, k, v, hash, key, val);
            System.arraycopy(array, idx+2, newArray, idx+1, n-idx-2);
			return new BitmapIndexedNode(null, bitmap, kvbitmap ^ bit, newArray);
		} else {
			int n = Integer.bitCount(bitmap) + Integer.bitCount(kvbitmap);
            addedLeaf.val = addedLeaf; 
			Object[] newArray = new Object[n + 2];
			System.arraycopy(array, 0, newArray, 0, idx);
			newArray[idx] = key;
			newArray[idx+1] = val;
			System.arraycopy(array, idx, newArray, idx+2, n-idx);
			return new BitmapIndexedNode(null, bitmap | bit, kvbitmap | bit, newArray);
		}
	}

	public INode without(int shift, int hash, Object key){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return this;
		int idx = index(bit);
		if((kvbitmap & bit) == 0) {
            INode node = (INode) array[idx];
			INode n = node.without(shift + 5, hash, key);
			if (n == node)
				return this;
			if (n != null)
				return new BitmapIndexedNode(null, bitmap, kvbitmap, cloneAndSet(array, idx, n));
			if (bitmap == bit) 
				return null;
			return new BitmapIndexedNode(null, bitmap ^ bit, kvbitmap, removeNode(array, idx));
		}
		if(Util.equiv(key, array[idx]))
			// TODO: collapse
			return new BitmapIndexedNode(null, bitmap ^ bit, kvbitmap ^ bit, removePair(array, idx));
		return this;
	}
	
	public IMapEntry find(int shift, int hash, Object key){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return null;
		int idx = index(bit);
		if((kvbitmap & bit) == 0)
			return ((INode) array[idx]).find(shift + 5, hash, key);
        Object k = array[idx];
        Object v = array[idx+1];
		if(Util.equiv(key, k))
			return new MapEntry(k, v);
		return null;
	}

	public Object find(int shift, int hash, Object key, Object notFound){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return notFound;
		int idx = index(bit);
		if((kvbitmap & bit) == 0)
            return ((INode) array[idx]).find(shift + 5, hash, key, notFound);
        Object k = array[idx];
        Object v = array[idx+1];
        if(Util.equiv(key, k))
            return v;
		return notFound;
	}

	public ISeq nodeSeq(){
		return NodeSeq.create(array, bitmap, kvbitmap);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f, bitmap, kvbitmap);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array, f, init, bitmap, kvbitmap);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke(), bitmap, kvbitmap);
	}

	private BitmapIndexedNode ensureEditable(AtomicReference<Thread> edit){
		if(this.edit == edit)
			return this;
		int n = Integer.bitCount(bitmap) + Integer.bitCount(kvbitmap);
		Object[] newArray = new Object[n >= 0 ? n+2 : 4]; // make room for next assoc
		System.arraycopy(array, 0, newArray, 0, n);
		return new BitmapIndexedNode(edit, bitmap, kvbitmap, newArray);
	}
	
	private BitmapIndexedNode editAndSet(AtomicReference<Thread> edit, int i, Object a) {
		BitmapIndexedNode editable = ensureEditable(edit);
		editable.array[i] = a;
		return editable;
	}

	private BitmapIndexedNode editAndSet(AtomicReference<Thread> edit, int i, Object a, int j, Object b) {
		BitmapIndexedNode editable = ensureEditable(edit);
		editable.array[i] = a;
		editable.array[j] = b;
		return editable;
	}

    private BitmapIndexedNode editAndRemovePair(AtomicReference<Thread> edit, int bit, int i) {
        if (bitmap == bit) 
            return null;
        BitmapIndexedNode editable = ensureEditable(edit);
        editable.bitmap &= ~bit;
        editable.kvbitmap &= ~bit;
        System.arraycopy(editable.array, i+2, editable.array, i, editable.array.length - i - 2);
        editable.array[editable.array.length - 2] = null;
        editable.array[editable.array.length - 1] = null;
        return editable;
    }

    private BitmapIndexedNode editAndRemoveNode(AtomicReference<Thread> edit, int bit, int i) {
        if (bitmap == bit) 
            return null;
        BitmapIndexedNode editable = ensureEditable(edit);
        editable.bitmap &= ~bit;
        editable.kvbitmap &= ~bit;
        System.arraycopy(editable.array, i+1, editable.array, i, editable.array.length - i - 1);
        editable.array[editable.array.length - 1] = null;
        return editable;
    }

    public INode assoc(AtomicReference<Thread> edit, int shift, int hash, Object key, Object val, Box addedLeaf){
		int bit = bitpos(hash, shift);
		int idx = index(bit);
		if((bitmap & bit) != 0) {
	        if((kvbitmap & bit) == 0) {
                INode node = (INode) array[idx];
                INode n = node.assoc(edit, shift + 5, hash, key, val, addedLeaf);
				if(n == node)
					return this;
				return editAndSet(edit, idx, n);
			} 
	        Object k = array[idx];
            Object v = array[idx+1];
            if(Util.equiv(key, k)) {
                if(val == v)
    					return this;
				return editAndSet(edit, idx+1, val);
			} 
			addedLeaf.val = addedLeaf;
			BitmapIndexedNode editable = ensureEditable(edit);
			editable.kvbitmap ^= bit;
            editable.array[idx] = createNode(edit, shift + 5, k, v, hash, key, val);
			System.arraycopy(editable.array, idx+2, editable.array, idx+1, editable.array.length - idx - 2);
			editable.array[editable.array.length - 1] = null;
			return editable;
		} else {
	        int n = Integer.bitCount(bitmap) + Integer.bitCount(kvbitmap);
            addedLeaf.val = addedLeaf;
            BitmapIndexedNode editable = ensureEditable(edit);
	        if(n+2 <= array.length) {
				System.arraycopy(editable.array, idx, editable.array, idx+2, n-idx);
				editable.array[idx] = key;
				editable.array[idx+1] = val;
                editable.bitmap |= bit;
                editable.kvbitmap |= bit;
				return editable;
			}
	        Object[] newArray = new Object[n+4];
			System.arraycopy(array, 0, newArray, 0, idx);
			newArray[idx] = key;
			newArray[idx+1] = val;
			System.arraycopy(array, idx, newArray, idx+2, n-idx);
			editable.array = newArray;
            editable.bitmap |= bit;
            editable.kvbitmap |= bit;
			return editable;
		}
	}

	public INode without(AtomicReference<Thread> edit, int shift, int hash, Object key, Box removedLeaf){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return this;
		int idx = index(bit);
        if((kvbitmap & bit) == 0) {
            INode node = (INode) array[idx];
            INode n = node.without(edit, shift + 5, hash, key, removedLeaf);
            if (n == node)
				return this;
			if (n != null)
				return editAndSet(edit, idx, n); 
			if (bitmap == bit) 
				return null;
			return editAndRemoveNode(edit, bit, idx); 
		}
		if(Util.equiv(key, array[idx])) {
			removedLeaf.val = removedLeaf;
			// TODO: collapse
			return editAndRemovePair(edit, bit, idx); 			
		}
		return this;
	}
}

final static class HashCollisionNode implements INode{

	final int hash;
	int count;
	Object[] array;
	final AtomicReference<Thread> edit;

	HashCollisionNode(AtomicReference<Thread> edit, int hash, int count, Object... array){
		this.edit = edit;
		this.hash = hash;
		this.count = count;
		this.array = array;
	}

	public INode assoc(int shift, int hash, Object key, Object val, Box addedLeaf){
		if(hash == this.hash) {
			int idx = findIndex(key);
			if(idx != -1) {
				if(array[idx + 1] == val)
					return this;
				return new HashCollisionNode(null, hash, count, cloneAndSet(array, idx + 1, val));
			}
			Object[] newArray = new Object[2 * (count + 1)];
			System.arraycopy(array, 0, newArray, 0, 2 * count);
			newArray[2 * count] = key;
			newArray[2 * count + 1] = val;
			addedLeaf.val = addedLeaf;
			return new HashCollisionNode(edit, hash, count + 1, newArray);
		}
		// nest it in a bitmap node
		return new BitmapIndexedNode(null, bitpos(this.hash, shift), 0, new Object[] {this})
			.assoc(shift, hash, key, val, addedLeaf);
	}

	public INode without(int shift, int hash, Object key){
		int idx = findIndex(key);
		if(idx == -1)
			return this;
		if(count == 1)
			return null;
		return new HashCollisionNode(null, hash, count - 1, removePair(array, idx/2));
	}

	public IMapEntry find(int shift, int hash, Object key){
		int idx = findIndex(key);
		if(idx < 0)
			return null;
		if(Util.equiv(key, array[idx]))
			return new MapEntry(array[idx], array[idx+1]);
		return null;
	}

	public Object find(int shift, int hash, Object key, Object notFound){
		int idx = findIndex(key);
		if(idx < 0)
			return notFound;
		if(Util.equiv(key, array[idx]))
			return array[idx+1];
		return notFound;
	}

	public ISeq nodeSeq(){
		return NodeSeq.create(array, (1 << count) - 1, (1 << count) - 1);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f, (1 << count) - 1, (1 << count) - 1);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array,f,init, (1 << count) - 1, (1 << count) - 1);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke(), (1 << count) - 1, (1 << count) - 1);
	}

	public int findIndex(Object key){
		for(int i = 0; i < 2*count; i+=2)
			{
			if(Util.equiv(key, array[i]))
				return i;
			}
		return -1;
	}

	private HashCollisionNode ensureEditable(AtomicReference<Thread> edit){
		if(this.edit == edit)
			return this;
		Object[] newArray = new Object[2*(count+1)]; // make room for next assoc
		System.arraycopy(array, 0, newArray, 0, 2*count);
		return new HashCollisionNode(edit, hash, count, newArray);
	}

	private HashCollisionNode ensureEditable(AtomicReference<Thread> edit, int count, Object[] array){
		if(this.edit == edit) {
			this.array = array;
			this.count = count;
			return this;
		}
		return new HashCollisionNode(edit, hash, count, array);
	}

	private HashCollisionNode editAndSet(AtomicReference<Thread> edit, int i, Object a) {
		HashCollisionNode editable = ensureEditable(edit);
		editable.array[i] = a;
		return editable;
	}

	private HashCollisionNode editAndSet(AtomicReference<Thread> edit, int i, Object a, int j, Object b) {
		HashCollisionNode editable = ensureEditable(edit);
		editable.array[i] = a;
		editable.array[j] = b;
		return editable;
	}


	public INode assoc(AtomicReference<Thread> edit, int shift, int hash, Object key, Object val, Box addedLeaf){
		if(hash == this.hash) {
			int idx = findIndex(key);
			if(idx != -1) {
				if(array[idx + 1] == val)
					return this;
				return editAndSet(edit, idx+1, val); 
			}
			if (array.length > 2*count) {
				addedLeaf.val = addedLeaf;
				HashCollisionNode editable = editAndSet(edit, 2*count, key, 2*count+1, val);
				editable.count++;
				return editable;
			}
			Object[] newArray = new Object[array.length + 2];
			System.arraycopy(array, 0, newArray, 0, array.length);
			newArray[array.length] = key;
			newArray[array.length + 1] = val;
			addedLeaf.val = addedLeaf;
			return ensureEditable(edit, count + 1, newArray);
		}
		// nest it in a bitmap node
		return new BitmapIndexedNode(edit, bitpos(this.hash, shift), 0, new Object[] {this, null, null})
			.assoc(edit, shift, hash, key, val, addedLeaf);
	}	

	public INode without(AtomicReference<Thread> edit, int shift, int hash, Object key, Box removedLeaf){
		int idx = findIndex(key);
		if(idx == -1)
			return this;
		removedLeaf.val = removedLeaf;
		if(count == 1)
			return null;
		HashCollisionNode editable = ensureEditable(edit);
		editable.array[idx] = editable.array[2*count-2];
		editable.array[idx+1] = editable.array[2*count-1];
		editable.array[2*count-2] = editable.array[2*count-1] = null;
		editable.count--;
		return editable;
	}
}

/*
public static void main(String[] args){
	try
		{
		ArrayList words = new ArrayList();
		Scanner s = new Scanner(new File(args[0]));
		s.useDelimiter(Pattern.compile("\\W"));
		while(s.hasNext())
			{
			String word = s.next();
			words.add(word);
			}
		System.out.println("words: " + words.size());
		IPersistentMap map = PersistentHashMap.EMPTY;
		//IPersistentMap map = new PersistentTreeMap();
		//Map ht = new Hashtable();
		Map ht = new HashMap();
		Random rand;

		System.out.println("Building map");
		long startTime = System.nanoTime();
		for(Object word5 : words)
			{
			map = map.assoc(word5, word5);
			}
		rand = new Random(42);
		IPersistentMap snapshotMap = map;
		for(int i = 0; i < words.size() / 200; i++)
			{
			map = map.without(words.get(rand.nextInt(words.size() / 2)));
			}
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("count = " + map.count() + ", time: " + estimatedTime / 1000000);

		System.out.println("Building ht");
		startTime = System.nanoTime();
		for(Object word1 : words)
			{
			ht.put(word1, word1);
			}
		rand = new Random(42);
		for(int i = 0; i < words.size() / 200; i++)
			{
			ht.remove(words.get(rand.nextInt(words.size() / 2)));
			}
		estimatedTime = System.nanoTime() - startTime;
		System.out.println("count = " + ht.size() + ", time: " + estimatedTime / 1000000);

		System.out.println("map lookup");
		startTime = System.nanoTime();
		int c = 0;
		for(Object word2 : words)
			{
			if(!map.contains(word2))
				++c;
			}
		estimatedTime = System.nanoTime() - startTime;
		System.out.println("notfound = " + c + ", time: " + estimatedTime / 1000000);
		System.out.println("ht lookup");
		startTime = System.nanoTime();
		c = 0;
		for(Object word3 : words)
			{
			if(!ht.containsKey(word3))
				++c;
			}
		estimatedTime = System.nanoTime() - startTime;
		System.out.println("notfound = " + c + ", time: " + estimatedTime / 1000000);
		System.out.println("snapshotMap lookup");
		startTime = System.nanoTime();
		c = 0;
		for(Object word4 : words)
			{
			if(!snapshotMap.contains(word4))
				++c;
			}
		estimatedTime = System.nanoTime() - startTime;
		System.out.println("notfound = " + c + ", time: " + estimatedTime / 1000000);
		}
	catch(FileNotFoundException e)
		{
		e.printStackTrace();
		}

}
*/

private static INode[] cloneAndSet(INode[] array, int i, INode a) {
	INode[] clone = array.clone();
	clone[i] = a;
	return clone;
}

private static Object[] cloneAndSet(Object[] array, int i, Object a) {
	Object[] clone = array.clone();
	clone[i] = a;
	return clone;
}

private static Object[] cloneAndSet(Object[] array, int i, Object a, int j, Object b) {
    Object[] clone = array.clone();
    clone[i] = a;
    clone[j] = b;
    return clone;
}

private static Object[] removePair(Object[] array, int i) {
    Object[] newArray = new Object[array.length - 2];
    System.arraycopy(array, 0, newArray, 0, i);
    System.arraycopy(array, i+2, newArray, i, newArray.length - i);
    return newArray;
}

private static Object[] removeNode(Object[] array, int i) {
    Object[] newArray = new Object[array.length - 1];
    System.arraycopy(array, 0, newArray, 0, i);
    System.arraycopy(array, i+1, newArray, i, newArray.length - i);
    return newArray;
}

private static INode createNode(int shift, Object key1, Object val1, int key2hash, Object key2, Object val2) {
	int key1hash = hash(key1);
	if(key1hash == key2hash)
		return new HashCollisionNode(null, key1hash, 2, new Object[] {key1, val1, key2, val2});
	Box addedLeaf = new Box(null);
	AtomicReference<Thread> edit = new AtomicReference<Thread>();
	return BitmapIndexedNode.EMPTY
		.assoc(edit, shift, key1hash, key1, val1, addedLeaf)
		.assoc(edit, shift, key2hash, key2, val2, addedLeaf);
}

private static INode createNode(AtomicReference<Thread> edit, int shift, Object key1, Object val1, int key2hash, Object key2, Object val2) {
	int key1hash = hash(key1);
	if(key1hash == key2hash)
		return new HashCollisionNode(null, key1hash, 2, new Object[] {key1, val1, key2, val2});
	Box addedLeaf = new Box(null);
	return BitmapIndexedNode.EMPTY
		.assoc(edit, shift, key1hash, key1, val1, addedLeaf)
		.assoc(edit, shift, key2hash, key2, val2, addedLeaf);
}

private static int bitpos(int hash, int shift){
	return 1 << mask(hash, shift);
}

static final class NodeIter implements Iterator {
    private static final Object NULL = new Object();
    final Object[] array;
    final IFn f;
    private int i = 0;
    private Object nextEntry = NULL;
    private Iterator nextIter;
    private int bitmap;
    private int kvbitmap;

    NodeIter(Object[] array, IFn f, int bitmap, int kvbitmap){
        this.array = array;
        this.f = f;
        this.bitmap = bitmap;
        this.kvbitmap = kvbitmap;
    }

    private boolean advance(){
        for (;bitmap != 0; bitmap >>>= 1, kvbitmap >>>= 1)
        {
            if ((bitmap & 1) == 0) continue;
            if ((kvbitmap & 1) == 1)
            {
                Object key = array[i];
                Object val = array[i+1];
                i += 2;
                bitmap >>>= 1;
                kvbitmap >>>=1;
                nextEntry = f.invoke(key, val);
                return true;
            }
            Iterator iter = ((INode) array[i]).iterator(f);
            i+=1;
            if(iter != null && iter.hasNext())
            {
                bitmap >>>= 1;
                kvbitmap >>>=1;
                nextIter = iter;
                return true;
            }
        }
        return false;
    }

    public boolean hasNext(){
        if (nextEntry != NULL || nextIter != null)
            return true;
        return advance();
    }

    public Object next(){
        Object ret = nextEntry;
        if(ret != NULL)
        {
            nextEntry = NULL;
            return ret;
        }
        else if(nextIter != null)
        {
            ret = nextIter.next();
            if(! nextIter.hasNext())
                nextIter = null;
            return ret;
        }
        else if(advance())
            return next();
        throw new NoSuchElementException();
    }

    public void remove(){
        throw new UnsupportedOperationException();
    }
}

static final class NodeSeq extends ASeq {
	final Object[] array;
	final int bitmap;
	final int kvbitmap;
	final int i;
	final ISeq s;
	
	NodeSeq(Object[] array, int i, int bitmap, int kvbitmap) {
		this(null, array, i, null, bitmap, kvbitmap);
	}

	static ISeq create(Object[] array, int bitmap, int kvbitmap) {
		return create(array, 0, null, bitmap, kvbitmap);
	}

    static public Object kvreduce(Object[] array, IFn f, Object init, int bitmap, int kvbitmap){
        int i = 0;
        for(; bitmap !=0; bitmap >>>= 1, kvbitmap >>>= 1) {
            if ((bitmap & 1) == 0) continue;
            if ((kvbitmap & 1) == 1) {
                init = f.invoke(init, array[i], array[i+1]);
                i+=2;
            } else {
                init = ((INode) array[i]).kvreduce(f,init);
                i++;
            }
            if(RT.isReduced(init))
                return init;
        }
        return init;
    }

	private static ISeq create(Object[] array, int i, ISeq s, int bitmap, int kvbitmap) {
		if(s != null)
			return new NodeSeq(null, array, i, s, bitmap, kvbitmap);
		for(; bitmap != 0; bitmap >>>= 1, kvbitmap >>>= 1) {
		    if ((bitmap & 1) == 0) continue;
			if ((kvbitmap & 1) == 1)
				return new NodeSeq(null, array, i, null, bitmap >>> 1, kvbitmap >>> 1);
			ISeq nodeSeq = ((INode) array[i]).nodeSeq();
			if(nodeSeq != null)
			    return new NodeSeq(null, array, i+1, nodeSeq, bitmap >>> 1, kvbitmap >>> 1);
			i++;
		}
		return null;
	}
	
	NodeSeq(IPersistentMap meta, Object[] array, int i, ISeq s, int bitmap, int kvbitmap) {
		super(meta);
		this.bitmap = bitmap;
		this.kvbitmap = kvbitmap;
		this.array = array;
		this.i = i;
		this.s = s;
	}

	public Obj withMeta(IPersistentMap meta) {
		return new NodeSeq(meta, array, i, s, bitmap, kvbitmap);
	}

	public Object first() {
		if(s != null)
			return s.first();
		return new MapEntry(array[i], array[i+1]);
	}

	public ISeq next() {
		if(s != null)
			return create(array, i, s.next(), bitmap, kvbitmap);
		return create(array, i + 2, null, bitmap, kvbitmap);
	}
}

}

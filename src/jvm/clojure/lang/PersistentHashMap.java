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
final boolean hasNull;
final Object nullValue;
final IPersistentMap _meta;

final public static PersistentHashMap EMPTY = new PersistentHashMap(0, null, false, null);
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

PersistentHashMap(int count, INode root, boolean hasNull, Object nullValue){
	this.count = count;
	this.root = root;
	this.hasNull = hasNull;
	this.nullValue = nullValue;
	this._meta = null;
}

public PersistentHashMap(IPersistentMap meta, int count, INode root, boolean hasNull, Object nullValue){
	this._meta = meta;
	this.count = count;
	this.root = root;
	this.hasNull = hasNull;
	this.nullValue = nullValue;
}

static int hash(Object k){
	return Util.hasheq(k);
}

public boolean containsKey(Object key){
	if(key == null)
		return hasNull;
	return (root != null) ? root.find(0, hash(key), key, NOT_FOUND) != NOT_FOUND : false;
}

public IMapEntry entryAt(Object key){
	if(key == null)
		return hasNull ? new MapEntry(null, nullValue) : null;
	return (root != null) ? root.find(0, hash(key), key) : null;
}

public IPersistentMap assoc(Object key, Object val){
	if(key == null) {
		if(hasNull && val == nullValue)
			return this;
		return new PersistentHashMap(meta(), hasNull ? count : count + 1, root, true, val);
	}
	PersistentUpdater updater = new PersistentUpdater();
	int hash = hash(key);
    INode newroot; 
	if (root == null) {
	    updater.countDelta++;
	    newroot = updater.createBitmap(bitpos(hash(key), 0), key, val);
	} else {
	    newroot = root.assoc(updater, 0, hash, key, val);
	    if(newroot == root)
	        return this;
	}
	return new PersistentHashMap(meta(), count + updater.countDelta, newroot, hasNull, nullValue);
}

public Object valAt(Object key, Object notFound){
	if(key == null)
		return hasNull ? nullValue : notFound;
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
	if(key == null)
		return hasNull ? new PersistentHashMap(meta(), count - 1, root, false, null) : this;
	if(root == null)
		return this;
	
	PersistentUpdater updater = new PersistentUpdater();
	INode newroot = root.without(updater, 0, hash(key), key);
	if(newroot == root)
		return this;
	return new PersistentHashMap(meta(), count + updater.countDelta, newroot, hasNull, nullValue); 
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
    final Iterator rootIter = (root == null) ? EMPTY_ITER : root.iterator(f);
    if(hasNull) {
        return new Iterator() {
            private boolean seen = false;
            public boolean hasNext() {
                if (!seen)
                    return true;
                else
                    return rootIter.hasNext();
            }

            public Object next(){
                if (!seen) {
                    seen = true;
                    return f.invoke(null, nullValue);
                } else
                    return rootIter.next();
            }

            public void remove(){
                throw new UnsupportedOperationException();
            }
        };
    }
    else
        return rootIter;
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
    init = hasNull?f.invoke(init,null,nullValue):init;
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
			return hasNull?
			       combinef.invoke(ret,reducef.invoke(combinef.invoke(),null,nullValue))
			       :ret;
		}
	};
	return fjinvoke.invoke(top);
}

public int count(){
	return count;
}

public ISeq seq(){
	ISeq s = root != null ? root.nodeSeq() : null; 
	return hasNull ? new Cons(new MapEntry(null, nullValue), s) : s;
}

public IPersistentCollection empty(){
	return EMPTY.withMeta(meta());	
}

static int mask(int hash, int shift){
	//return ((hash << shift) >>> 27);// & 0x01f;
	return (hash >>> shift) & 0x01f;
}

public PersistentHashMap withMeta(IPersistentMap meta){
	return new PersistentHashMap(meta, count, root, hasNull, nullValue);
}

public TransientHashMap asTransient() {
	return new TransientHashMap(this);
}

public IPersistentMap meta(){
	return _meta;
}

static final class TransientHashMap extends ATransientMap {
	final TransientUpdater updater;
	volatile INode root;
	volatile int count;
	volatile boolean hasNull;
	volatile Object nullValue;

	TransientHashMap(PersistentHashMap m) {
		this(new AtomicReference<Thread>(Thread.currentThread()), m.root, m.count, m.hasNull, m.nullValue);
	}
	
	TransientHashMap(AtomicReference<Thread> edit, INode root, int count, boolean hasNull, Object nullValue) {
		this.updater = new TransientUpdater(edit);
		this.root = root; 
		this.count = count; 
		this.hasNull = hasNull;
		this.nullValue = nullValue;
	}

	ITransientMap doAssoc(Object key, Object val) {
		if (key == null) {
			if (this.nullValue != val)
				this.nullValue = val;
			if (!hasNull) {
				this.count++;
				this.hasNull = true;
			}
			return this;
		}
		int hash = hash(key);
	    INode newroot; 
	    if (root == null) {
	        updater.countDelta++;
	        newroot = updater.createBitmap(bitpos(hash, 0), key, val);
	    } else {
	        newroot = root.assoc(updater, 0, hash, key, val);
	    }

		if (newroot != this.root)
			this.root = newroot; 
		return this;
	}

	ITransientMap doWithout(Object key) {
		if (key == null) {
			if (!hasNull) return this;
			hasNull = false;
			nullValue = null;
			this.count--;
			return this;
		}
		if (root == null) return this;
		INode n = root.without(updater, 0, hash(key), key);
		if (n != root)
			this.root = n;
		return this;
	}

	IPersistentMap doPersistent() {
		updater.edit.set(null);
		return new PersistentHashMap(doCount(), root, hasNull, nullValue);
	}

	Object doValAt(Object key, Object notFound) {
		if (key == null)
			if (hasNull)
				return nullValue;
			else
				return notFound;
		if (root == null)
			return notFound;
		return root.find(0, hash(key), key, notFound);
	}

	int doCount() {
		return count + updater.countDelta;
	}
	
	void ensureEditable(){
		if(updater.edit.get() == null)
			throw new IllegalAccessError("Transient used after persistent! call");
	}
}

static interface INode extends Serializable {
	IMapEntry find(int shift, int hash, Object key);

	Object find(int shift, int hash, Object key, Object notFound);

	ISeq nodeSeq();

	INode assoc(NodeUpdater updater, int shift, int hash, Object key, Object val);

	INode without(NodeUpdater updater, int shift, int hash, Object key);

    public Object kvreduce(IFn f, Object init);

	Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin);

    // returns the result of (f [k v]) for each iterated element
    Iterator iterator(IFn f);
}

static abstract class NodeUpdater {
    int countDelta = 0;
    
    abstract ArrayNode editable(ArrayNode arrayNode);

    abstract BitmapIndexedNode editable(BitmapIndexedNode node);

    abstract BitmapIndexedNode allocSlot(BitmapIndexedNode node, int i, int n);

    abstract BitmapIndexedNode freeSlot(BitmapIndexedNode node, int i, int bit);
    
    abstract HashCollisionNode editable(HashCollisionNode hashCollisionNode);

    abstract HashCollisionNode allocSlot(HashCollisionNode hashCollisionNode);

    abstract HashCollisionNode freeSlot(HashCollisionNode node, int idx);

    abstract HashCollisionNode createCollision(int hash, Object key1, Object val1,
            Object key2, Object val2);

    abstract BitmapIndexedNode createBitmap(int bitmap, Object key1, Object val1, Object key2,
            Object val2);

    abstract BitmapIndexedNode createBitmap(int bitmap, Object key, Object val);
}

final static class ArrayNode implements INode{
	int count;
	final INode[] array;
	final AtomicReference<Thread> edit;

	ArrayNode(AtomicReference<Thread> edit, int count, INode[] array){
		this.array = array;
		this.edit = edit;
		this.count = count;
	}

	public IMapEntry find(int shift, int hash, Object key){
		int idx = mask(hash, shift);
		INode node = array[idx];
		if(node == null)
			return null;
		return node.find(shift + 5, hash, key); 
	}

	public Object find(int shift, int hash, Object key, Object notFound){
		int idx = mask(hash, shift);
		INode node = array[idx];
		if(node == null)
			return notFound;
		return node.find(shift + 5, hash, key, notFound); 
	}
	
	public ISeq nodeSeq(){
		return Seq.create(array);
	}

    public Iterator iterator(IFn f){
        return new Iter(array, f);
    }

    public Object kvreduce(IFn f, Object init){
        for(INode node : array){
            if(node != null){
                init = node.kvreduce(f,init);
	            if(RT.isReduced(init))
		            return init;
	            }
	        }
        return init;
    }

	public Object fold(final IFn combinef, final IFn reducef,
	                   final IFn fjtask, final IFn fjfork, final IFn fjjoin){
		List<Callable> tasks = new ArrayList();
		for(final INode node : array){
			if(node != null){
				tasks.add(new Callable(){
					public Object call() throws Exception{
						return node.fold(combinef, reducef, fjtask, fjfork, fjjoin);
					}
				});
				}
			}

		return foldTasks(tasks,combinef,fjtask,fjfork,fjjoin);
		}

	static public Object foldTasks(List<Callable> tasks, final IFn combinef,
	                          final IFn fjtask, final IFn fjfork, final IFn fjjoin){

		if(tasks.isEmpty())
			return combinef.invoke();

		if(tasks.size() == 1){
			Object ret = null;
			try
				{
				return tasks.get(0).call();
				}
			catch(Exception e)
				{
				throw Util.sneakyThrow(e);
				}
			}

		List<Callable> t1 = tasks.subList(0,tasks.size()/2);
		final List<Callable> t2 = tasks.subList(tasks.size()/2, tasks.size());

		Object forked = fjfork.invoke(fjtask.invoke(new Callable() {
			public Object call() throws Exception{
				return foldTasks(t2,combinef,fjtask,fjfork,fjjoin);
			}
		}));

		return combinef.invoke(foldTasks(t1,combinef,fjtask,fjfork,fjjoin),fjjoin.invoke(forked));
	}


	private ArrayNode ensureEditable(AtomicReference<Thread> edit){
		if(this.edit == edit)
			return this;
		return new ArrayNode(edit, count, this.array.clone());
	}
	
	private ArrayNode editAndSet(NodeUpdater updater, int i, INode n){
		ArrayNode editable = updater.editable(this);
		editable.array[i] = n;
		return editable;
	}


	private INode pack(AtomicReference<Thread> edit, int idx) {
		Object[] newArray = new Object[2*(count - 1)];
		int j = 1;
		int bitmap = 0;
		for(int i = 0; i < idx; i++)
			if (array[i] != null) {
				newArray[j] = array[i];
				bitmap |= 1 << i;
				j += 2;
			}
		for(int i = idx + 1; i < array.length; i++)
			if (array[i] != null) {
				newArray[j] = array[i];
				bitmap |= 1 << i;
				j += 2;
			}
		return new BitmapIndexedNode(edit, bitmap, newArray);
	}

	public INode assoc(NodeUpdater updater, int shift, int hash, Object key, Object val){
		int idx = mask(hash, shift);
		INode node = array[idx];
		if(node == null) {
			ArrayNode editable = editAndSet(updater, idx, updater.createBitmap(bitpos(hash, shift+5), key, val));
            updater.countDelta++;
			editable.count++;
			return editable;			
		}
		INode n = node.assoc(updater, shift + 5, hash, key, val);
		if(n == node)
			return this;
		return editAndSet(updater, idx, n);
	}	

	public INode without(NodeUpdater updater, int shift, int hash, Object key){
		int idx = mask(hash, shift);
		INode node = array[idx];
		if(node == null)
			return this;
		INode n = node.without(updater, shift + 5, hash, key);
		if(n == node)
			return this;
		if(n == null) {
			if (count <= 8) // shrink
				return pack(edit, idx);
			ArrayNode editable = editAndSet(updater, idx, n);
			editable.count--;
			return editable;
		}
		return editAndSet(updater, idx, n);
	}
	
	static class Seq extends ASeq {
		final INode[] nodes;
		final int i;
		final ISeq s; 
		
		static ISeq create(INode[] nodes) {
			return create(null, nodes, 0, null);
		}
		
		private static ISeq create(IPersistentMap meta, INode[] nodes, int i, ISeq s) {
			if (s != null)
				return new Seq(meta, nodes, i, s);
			for(int j = i; j < nodes.length; j++)
				if (nodes[j] != null) {
					ISeq ns = nodes[j].nodeSeq();
					if (ns != null)
						return new Seq(meta, nodes, j + 1, ns);
				}
			return null;
		}
		
		private Seq(IPersistentMap meta, INode[] nodes, int i, ISeq s) {
			super(meta);
			this.nodes = nodes;
			this.i = i;
			this.s = s;
		}

		public Obj withMeta(IPersistentMap meta) {
			return new Seq(meta, nodes, i, s);
		}

		public Object first() {
			return s.first();
		}

		public ISeq next() {
			return create(null, nodes, i, s.next());
		}
		
	}

    static class Iter implements Iterator {
        private final INode[] array;
        private final IFn f;
        private int i = 0;
        private Iterator nestedIter;

        private Iter(INode[] array, IFn f){
            this.array = array;
            this.f = f;
        }

        public boolean hasNext(){
            while(true)
            {
                if(nestedIter != null)
                    if(nestedIter.hasNext())
                        return true;
                    else
                        nestedIter = null;

                if(i < array.length)
                {
                    INode node = array[i++];
                    if (node != null)
                        nestedIter = node.iterator(f);
                }
                else
                    return false;
            }
        }

        public Object next(){
            if(hasNext())
                return nestedIter.next();
            else
                throw new NoSuchElementException();
        }

        public void remove(){
            throw new UnsupportedOperationException();
        }
    }
}

final static class BitmapIndexedNode implements INode{
	int bitmap;
	Object[] array;
	final AtomicReference<Thread> edit;

	final int index(int bit){
		return Integer.bitCount(bitmap & (bit - 1));
	}

	BitmapIndexedNode(AtomicReference<Thread> edit, int bitmap, Object[] array){
		this.bitmap = bitmap;
		this.array = array;
		this.edit = edit;
	}
	
	public IMapEntry find(int shift, int hash, Object key){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return null;
		int idx = index(bit);
		Object keyOrNull = array[2*idx];
		Object valOrNode = array[2*idx+1];
		if(keyOrNull == null)
			return ((INode) valOrNode).find(shift + 5, hash, key);
		if(Util.equiv(key, keyOrNull))
			return new MapEntry(keyOrNull, valOrNode);
		return null;
	}

	public Object find(int shift, int hash, Object key, Object notFound){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return notFound;
		int idx = index(bit);
		Object keyOrNull = array[2*idx];
		Object valOrNode = array[2*idx+1];
		if(keyOrNull == null)
			return ((INode) valOrNode).find(shift + 5, hash, key, notFound);
		if(Util.equiv(key, keyOrNull))
			return valOrNode;
		return notFound;
	}

	public ISeq nodeSeq(){
		return NodeSeq.create(array);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array,f,init);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke());
	}

	private BitmapIndexedNode editAndSet(NodeUpdater updater, int i, Object a) {
		BitmapIndexedNode editable = updater.editable(this);
		editable.array[i] = a;
		return editable;
	}

	private BitmapIndexedNode editAndSet(NodeUpdater updater, int i, Object a, int j, Object b) {
        BitmapIndexedNode editable = updater.editable(this);
		editable.array[i] = a;
		editable.array[j] = b;
		return editable;
	}

	public INode assoc(NodeUpdater updater, int shift, int hash, Object key, Object val){
		int bit = bitpos(hash, shift);
		int idx = index(bit);
		if((bitmap & bit) != 0) {
			Object keyOrNull = array[2*idx];
			Object valOrNode = array[2*idx+1];
			if(keyOrNull == null) {
				INode n = ((INode) valOrNode).assoc(updater, shift + 5, hash, key, val);
				if(n == valOrNode)
					return this;
				return editAndSet(updater, 2*idx+1, n);
			} 
			if(Util.equiv(key, keyOrNull)) {
				if(val == valOrNode)
					return this;
				return editAndSet(updater, 2*idx+1, val);
			}
			updater.countDelta++;
			return editAndSet(updater, 2*idx, null, 2*idx+1, 
					createNode(updater, shift + 5, keyOrNull, valOrNode, hash, key, val)); 
		} else {
            updater.countDelta++;
			int n = Integer.bitCount(bitmap);
			if(n >= 16) {
				INode[] nodes = new INode[32];
				int jdx = mask(hash, shift);
				nodes[jdx] = updater.createBitmap(bitpos(hash, shift+5), key, val);  
				int j = 0;
				for(int i = 0; i < 32; i++)
					if(((bitmap >>> i) & 1) != 0) {
						if (array[j] == null)
							nodes[i] = (INode) array[j+1];
						else
							nodes[i] = updater.createBitmap(bitpos(hash(array[j]), shift+5), array[j], array[j+1]);
						j += 2;
					}
				return new ArrayNode(edit, n + 1, nodes);
			}

			BitmapIndexedNode editable = updater.allocSlot(this, idx, n);
			editable.array[2*idx] = key;
            editable.array[2*idx+1] = val;
            editable.bitmap |= bit;

			return editable;
		}
	}

	private static Object createNode(NodeUpdater updater, int shift, Object key1,
            Object val1, int key2hash, Object key2, Object val2) {
        int key1hash = hash(key1);
        if(key1hash == key2hash)
            return updater.createCollision(key1hash, key1, val1, key2, val2);
        int bit1 = bitpos(key1hash, shift);
        int bit2 = bitpos(key2hash, shift);
        if ((bit1 - bit2) < 0) // beware of signed integers!
            return updater.createBitmap(bit1 | bit2, key1, val1, key2, val2);
        if (bit1 == bit2)
            return updater.createBitmap(bit1, null, createNode(updater, shift+5, key1, val1, key2hash, key2, val2));
        return updater.createBitmap(bit1 | bit2, key2, val2, key1, val1);
	}

    public INode without(NodeUpdater updater, int shift, int hash, Object key){
		int bit = bitpos(hash, shift);
		if((bitmap & bit) == 0)
			return this;
		int idx = index(bit);
		Object keyOrNull = array[2*idx];
		Object valOrNode = array[2*idx+1];
		if(keyOrNull == null) {
			INode n = ((INode) valOrNode).without(updater, shift + 5, hash, key);
			if (n == valOrNode)
				return this;
			if (n != null)
				return editAndSet(updater, 2*idx+1, n); 
	        if (bitmap == bit) return null;
			BitmapIndexedNode editable = updater.freeSlot(this, idx, bit);
			editable.bitmap ^= bit;
            return editable; 
		}
		if(Util.equiv(key, keyOrNull)) {
            updater.countDelta--;
            if (bitmap == bit) return null;
            BitmapIndexedNode editable = updater.freeSlot(this, idx, bit);
            editable.bitmap ^= bit;
            return editable; 
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
		return NodeSeq.create(array);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array,f,init);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke());
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

	private HashCollisionNode editAndSet(NodeUpdater updater, int i, Object a, int j, Object b) {
		HashCollisionNode editable = updater.editable(this);
		editable.array[i] = a;
		editable.array[j] = b;
		return editable;
	}


	public INode assoc(NodeUpdater updater, int shift, int hash, Object key, Object val){
		if(hash == this.hash) {
			int idx = findIndex(key);
			if(idx != -1) {
				if(array[idx + 1] == val)
					return this;
				return editAndSet(edit, idx+1, val); 
			}
            updater.countDelta++;
			HashCollisionNode editable = updater.allocSlot(this);
			editable.array[2*count] = key;
			editable.array[2*count+1] = val;
			editable.count++;
			return editable;
		}
		// nest it in a bitmap node
        int bitcn = bitpos(this.hash, shift);
        int bitkv = bitpos(hash, shift);

        if (bitcn < bitkv)
            return updater.createBitmap(bitcn | bitkv, null, this, key, val);
        return updater.createBitmap(bitcn | bitkv, key, val, null, this);
	}	

	public INode without(NodeUpdater updater, int shift, int hash, Object key){
		int idx = findIndex(key);
		if(idx == -1)
			return this;
		
        updater.countDelta--;
		if(count == 1)
			return null;
		HashCollisionNode editable = updater.freeSlot(this, idx);
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
	System.arraycopy(array, 0, newArray, 0, 2*i);
	System.arraycopy(array, 2*(i+1), newArray, 2*i, newArray.length - 2*i);
	return newArray;
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

    NodeIter(Object[] array, IFn f){
        this.array = array;
        this.f = f;
    }

    private boolean advance(){
        while (i<array.length)
        {
            Object key = array[i];
            Object nodeOrVal = array[i+1];
            i += 2;
            if (key != null)
            {
                nextEntry = f.invoke(key, nodeOrVal);
                return true;
            }
            else if(nodeOrVal != null)
            {
                Iterator iter = ((INode) nodeOrVal).iterator(f);
                if(iter != null && iter.hasNext())
                {
                    nextIter = iter;
                    return true;
                }
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
	final int i;
	final ISeq s;
	
	NodeSeq(Object[] array, int i) {
		this(null, array, i, null);
	}

	static ISeq create(Object[] array) {
		return create(array, 0, null);
	}

    static public Object kvreduce(Object[] array, IFn f, Object init){
         for(int i=0;i<array.length;i+=2)
             {
             if(array[i] != null)
                 init = f.invoke(init, array[i], array[i+1]);
             else
                 {
                 INode node = (INode) array[i+1];
                 if(node != null)
                     init = node.kvreduce(f,init);
                 }
             if(RT.isReduced(init))
	             return init;
             }
        return init;
    }

	private static ISeq create(Object[] array, int i, ISeq s) {
		if(s != null)
			return new NodeSeq(null, array, i, s);
		for(int j = i; j < array.length; j+=2) {
			if(array[j] != null)
				return new NodeSeq(null, array, j, null);
			INode node = (INode) array[j+1];
			if (node != null) {
				ISeq nodeSeq = node.nodeSeq();
				if(nodeSeq != null)
					return new NodeSeq(null, array, j + 2, nodeSeq);
			}
		}
		return null;
	}
	
	NodeSeq(IPersistentMap meta, Object[] array, int i, ISeq s) {
		super(meta);
		this.array = array;
		this.i = i;
		this.s = s;
	}

	public Obj withMeta(IPersistentMap meta) {
		return new NodeSeq(meta, array, i, s);
	}

	public Object first() {
		if(s != null)
			return s.first();
		return new MapEntry(array[i], array[i+1]);
	}

	public ISeq next() {
		if(s != null)
			return create(array, i, s.next());
		return create(array, i + 2, null);
	}
}

static final class PersistentUpdater extends NodeUpdater {
    public ArrayNode editable(ArrayNode node) {
        return new ArrayNode(null, node.count, node.array.clone());
    }

    public BitmapIndexedNode editable(BitmapIndexedNode node) {
        return new BitmapIndexedNode(null, node.bitmap, node.array.clone());
    }

    public BitmapIndexedNode allocSlot(BitmapIndexedNode node, int i, int n) {
        Object[] newArray = new Object[2*(n+1)];
        System.arraycopy(node.array, 0, newArray, 0, 2*i);
        System.arraycopy(node.array, 2*i, newArray, 2*(i+1), 2*(n - i));
        return new BitmapIndexedNode(null, node.bitmap, newArray);
    }

    public BitmapIndexedNode freeSlot(BitmapIndexedNode node, int i, int bit) {
        int n = node.array.length - 2;
        Object[] newArray = new Object[n];
        System.arraycopy(node.array, 0, newArray, 0, 2*i);
        System.arraycopy(node.array, 2*(i+1), newArray, 2*i, n - 2*i);
        return new BitmapIndexedNode(null, node.bitmap, newArray);
    }

    public HashCollisionNode createCollision(int hash, Object key1,
            Object val1, Object key2, Object val2) {
        return new HashCollisionNode(null, hash, 2, new Object[] { key1, val1, key2, val2 });
    }

    public BitmapIndexedNode createBitmap(int bitmap, Object key1, Object val1,
            Object key2, Object val2) {
        return new BitmapIndexedNode(null, bitmap, new Object[] { key1, val1, key2, val2 });
    }

    public HashCollisionNode editable(HashCollisionNode node) {
        return new HashCollisionNode(null, node.hash, node.count, node.array.clone());
    }

    public HashCollisionNode allocSlot(HashCollisionNode node) {
        Object[] newArray = new Object[2*(node.count+1)];
        System.arraycopy(node.array, 0, newArray, 0, 2*node.count);
        return new HashCollisionNode(null, node.hash, node.count, newArray);
    }
    
    public HashCollisionNode freeSlot(HashCollisionNode node, int idx) {
        int count = node.count;
        Object[] newArray = new Object[2*count-1];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx+2, newArray, idx, newArray.length - idx - 2);
        return new HashCollisionNode(null, node.hash, count, newArray);
    }

    public BitmapIndexedNode createBitmap(int bitmap, Object key, Object val) {
        return new BitmapIndexedNode(null, bitmap, new Object[] { key, val });
    }
};

static final class TransientUpdater extends NodeUpdater {
    AtomicReference<Thread> edit;
    TransientUpdater(AtomicReference<Thread> edit) {
        this.edit = edit;
    }
    public ArrayNode editable(ArrayNode node) {
        if(node.edit == edit)
            return node;
        return new ArrayNode(edit, node.count, node.array.clone());
    }
    
    public BitmapIndexedNode editable(BitmapIndexedNode node) {
        if(node.edit == edit)
            return node;
        return new BitmapIndexedNode(edit, node.bitmap, node.array.clone());
    }

    public BitmapIndexedNode allocSlot(BitmapIndexedNode node, int idx, int n) {
        if(node.edit == edit) {
            if(n*2 < node.array.length) {
                System.arraycopy(node.array, 2*idx, node.array, 2*(idx+1), 2*(n-idx));
                return node;
            }
            Object[] newArray = new Object[2*(n+1)];
            System.arraycopy(node.array, 0, newArray, 0, 2*idx);
            System.arraycopy(node.array, 2*idx, newArray, 2*(idx+1), 2*(n-idx));
            node.array = newArray;
            return node;
        }
        Object[] newArray = new Object[2*(n+1)];
        System.arraycopy(node.array, 0, newArray, 0, 2*idx);
        System.arraycopy(node.array, 2*idx, newArray, 2*(idx+1), 2*(n-idx));
        return new BitmapIndexedNode(edit, node.bitmap, newArray);
    }
    
    public HashCollisionNode createCollision(int hash,
            Object key1, Object val1, Object key2, Object val2) {
        return new HashCollisionNode(edit, hash, 2, new Object[] {key1, val1, key2, val2});
    }

    public BitmapIndexedNode createBitmap(int bitmap, Object key1, Object val1,
            Object key2, Object val2) {
        return new BitmapIndexedNode(edit, bitmap, new Object[] { key1, val1, key2, val2 });
    }

    public BitmapIndexedNode freeSlot(BitmapIndexedNode node, int i, int bit) {
        if (node.edit == edit) {
            System.arraycopy(node.array, 2*(i+1), node.array, 2*i, node.array.length - 2*(i+1));
            node.array[node.array.length - 2] = null;
            node.array[node.array.length - 1] = null;
            return node;
        }

        Object[] newArray = new Object[node.array.length - 2];
        System.arraycopy(node.array, 0, newArray, 0, 2*i);
        System.arraycopy(node.array, 2*(i+1), newArray, 2*i, node.array.length - 2*(i+1));
        return new BitmapIndexedNode(edit, node.bitmap, newArray);
    }

    public HashCollisionNode editable(HashCollisionNode node) {
        if (node.edit == edit) return node;
        return new HashCollisionNode(edit, node.hash, node.count, node.array.clone());
    }

    public HashCollisionNode allocSlot(HashCollisionNode node) {
        if (node.edit == edit) {
            if (node.array.length > 2*node.count) return node;
            Object[] newArray = new Object[2*(node.count+1)];
            System.arraycopy(node.array, 0, newArray, 0, 2*node.count);
            node.array = newArray;
            return node;
        }
        Object[] newArray = new Object[2*(node.count+1)];
        System.arraycopy(node.array, 0, newArray, 0, 2*node.count);
        return new HashCollisionNode(edit, node.hash, node.count, newArray);
    }
    
    public HashCollisionNode freeSlot(HashCollisionNode node, int idx) {
        int count = node.count;
        if (node.edit == edit) {
            node.array[idx] = node.array[2*count-2];
            node.array[idx+1] = node.array[2*count-1];
            node.array[2*count-2] = node.array[2*count-1] = null;
            return node;
        }
        Object[] newArray = new Object[2*count-1];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx+2, newArray, idx, newArray.length - idx - 2);
        return new HashCollisionNode(edit, node.hash, count, newArray);
    }

    public BitmapIndexedNode createBitmap(int bitmap, Object key, Object val) {
        return new BitmapIndexedNode(edit, bitmap, new Object[] { key, val });
    }
}
}

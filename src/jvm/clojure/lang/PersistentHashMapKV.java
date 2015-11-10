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

import clojure.lang.PersistentHashMapKV.BitmapIndexedNode;

/*
 A persistent rendition of Phil Bagwell's Hash Array Mapped Trie

 Uses path copying for persistence
 HashCollision leaves vs. extended hashing
 Node polymorphism vs. conditionals
 No sub-tree pools or root-resizing
 Any errors are my own
 */

public class PersistentHashMapKV extends APersistentMap implements IEditableCollection, IObj, IMapIterable {

final BitmapIndexedNode root;
final IPersistentMap _meta;

final public static PersistentHashMapKV EMPTY = new PersistentHashMapKV(null, BitmapIndexedNode.EMPTY);
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
public static PersistentHashMapKV create(Object... init){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < init.length; i += 2)
		{
		ret = ret.assoc(init[i], init[i + 1]);
		}
	return (PersistentHashMapKV) ret.persistent();
}

public static PersistentHashMapKV createWithCheck(Object... init){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < init.length; i += 2)
		{
		ret = ret.assoc(init[i], init[i + 1]);
		if(ret.count() != i/2 + 1)
			throw new IllegalArgumentException("Duplicate key: " + init[i]);
		}
	return (PersistentHashMapKV) ret.persistent();
}

static public PersistentHashMapKV create(ISeq items){
	ITransientMap ret = EMPTY.asTransient();
	for(; items != null; items = items.next().next())
		{
		if(items.next() == null)
			throw new IllegalArgumentException(String.format("No value supplied for key: %s", items.first()));
		ret = ret.assoc(items.first(), RT.second(items));
		}
	return (PersistentHashMapKV) ret.persistent();
}

static public PersistentHashMapKV createWithCheck(ISeq items){
	ITransientMap ret = EMPTY.asTransient();
	for(int i=0; items != null; items = items.next().next(), ++i)
		{
		if(items.next() == null)
			throw new IllegalArgumentException(String.format("No value supplied for key: %s", items.first()));
		ret = ret.assoc(items.first(), RT.second(items));
		if(ret.count() != i + 1)
			throw new IllegalArgumentException("Duplicate key: " + items.first());
		}
	return (PersistentHashMapKV) ret.persistent();
}

/*
 * @param init {key1,val1,key2,val2,...}
 */
public static PersistentHashMapKV create(IPersistentMap meta, Object... init){
	return create(init).withMeta(meta);
}

public PersistentHashMapKV(IPersistentMap meta, BitmapIndexedNode root){
	this._meta = meta;
	this.root = root;
}

public IPersistentCollection cons(Object o) {
//    if (o instanceof PersistentHashMapKV)
//        return merge((PersistentHashMapKV) o);
    return super.cons(o);
}

static int hash(Object k){
	return Util.hasheq(k);
}

public boolean containsKey(Object key){
	return root.find(0, hash(key), key, NOT_FOUND) != NOT_FOUND;
}

public IMapEntry entryAt(Object key){
	return root.find(0, hash(key), key);
}

public IPersistentMap assoc(Object key, Object val){
	BitmapIndexedNode newroot = root.assoc(PERSISTENT_NODE_EDITOR, 0, hash(key), key, val);
	if(newroot == root)
		return this;
	return new PersistentHashMapKV(meta(), newroot);
}

public Object valAt(Object key, Object notFound){
	return root.find(0, hash(key), key, notFound);
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
	BitmapIndexedNode newroot = root.without(PERSISTENT_NODE_EDITOR, 0, hash(key), key);
	if(newroot == root)
		return this;
	return new PersistentHashMapKV(meta(), newroot); 
}

private Iterator iterator(final IFn f){
    return root.iterator(f);
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
	init = root.kvreduce(f,init);
	if(RT.isReduced(init))
	    return ((IDeref)init).deref();
	else
	    return init;
}

public Object fold(long n, final IFn combinef, final IFn reducef,
                   IFn fjinvoke, final IFn fjtask, final IFn fjfork, final IFn fjjoin){
	//we are ignoring n for now
	Callable top = new Callable(){
		public Object call() throws Exception{
			Object ret = combinef.invoke();
			ret = combinef.invoke(ret, root.fold(combinef,reducef,fjtask,fjfork,fjjoin));
			return ret;
		}
	};
	return fjinvoke.invoke(top);
}

//public PersistentHashMapKV merge(PersistentHashMapKV src) {
//    TransientNodeEditor editor = new TransientNodeEditor(Thread.currentThread());
//    INode newRoot = merge(editor, 0, this.root, src.root);
//    return new PersistentHashMapKV(meta(), newRoot);
//}

public int count(){
	return root.count;
}

public ISeq seq(){
	return root.nodeSeq(); 
}

public IPersistentCollection empty(){
	return EMPTY.withMeta(meta());	
}

static int mask(int hash, int shift){
	//return ((hash << shift) >>> 27);// & 0x01f;
	return (hash >>> shift) & 0x01f;
}

public PersistentHashMapKV withMeta(IPersistentMap meta){
	return new PersistentHashMapKV(meta, root);
}

public TransientHashMap asTransient() {
	return new TransientHashMap(this);
}

public IPersistentMap meta(){
	return _meta;
}

static final class TransientHashMap extends ATransientMap {
	volatile TransientNodeEditor editor;
	volatile BitmapIndexedNode root;

	TransientHashMap(PersistentHashMapKV m) {
		this(new TransientNodeEditor(Thread.currentThread()), m.root);
	}
	
	TransientHashMap(TransientNodeEditor editor, BitmapIndexedNode root) {
		this.editor = editor;
		this.root = root; 
	}
	
	ITransientMap doAssoc(Object key, Object val) {
		BitmapIndexedNode n = root.assoc(editor, 0, hash(key), key, val);
		if (n != this.root)
			this.root = n;
		return this;
	}

	ITransientMap doWithout(Object key) {
		BitmapIndexedNode n = root.without(editor, 0, hash(key), key);
		if (n != root)
			this.root = n;
		return this;
	}

	IPersistentMap doPersistent() {
		editor.edit.set(null);
		editor = null;
		return new PersistentHashMapKV(null, root);
	}

	Object doValAt(Object key, Object notFound) {
		return root.find(0, hash(key), key, notFound);
	}

	int doCount() {
		return root.count;
	}
	
	void ensureEditable(){
		if(editor == null)
			throw new IllegalAccessError("Transient used after persistent! call");
	}
}

//static interface INode extends Serializable {
//    int count();
//    
//	IMapEntry find(int shift, int hash, Object key);
//
//	Object find(int shift, int hash, Object key, Object notFound);
//
//	ISeq nodeSeq();
//
//	INode assoc(INodeEditor editor, int shift, int hash, Object key, Object val);
//
//    INode without(INodeEditor editor, int shift, int hash, Object key);
//	
//    INode merge(TransientNodeEditor editor, int shift, INode src);
//    INode mergeBitmapNode(TransientNodeEditor editor, int shift, BitmapIndexedNode dst);
//    INode mergeCollisionNode(TransientNodeEditor editor, int shift, HashCollisionNode dst);
//
//    public Object kvreduce(IFn f, Object init);
//
//	Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin);
//	
//    // returns the result of (f [k v]) for each iterated element
//    Iterator iterator(IFn f);
//}

//static INode merge(TransientNodeEditor editor, int shift, INode dst, INode src) {
//    if (dst == src) return dst;
//    return dst.merge(editor, shift, src);
//}

static interface INodeEditor {
    BitmapIndexedNode edit(BitmapIndexedNode node);
    BitmapIndexedNode insert2(BitmapIndexedNode node, int idx);
    BitmapIndexedNode remove2(BitmapIndexedNode node, int idx);
    BitmapIndexedNode remove1(BitmapIndexedNode node, int idx);

    Object[] array(Object a);
    Object[] array(Object a, Object b);
    Object[] array(Object a, Object b, Object c);
    Object[] array(Object a, Object b, Object c, Object d);
    
    HashCollisionNode edit(HashCollisionNode node);
    HashCollisionNode insert(HashCollisionNode node);
    HashCollisionNode remove(HashCollisionNode node, int idx);
    BitmapIndexedNode insert1(BitmapIndexedNode node, int idx);
}

final static class TransientNodeEditor implements INodeEditor {
    final AtomicReference<Thread> edit;

    public TransientNodeEditor(Thread t) {
        edit = new AtomicReference<Thread>(t);
    }

    public BitmapIndexedNode edit(BitmapIndexedNode node) {
        if (node.edit == edit) return node;
        return new BitmapIndexedNode(edit, node.count, node.bitmap1, node.array.clone());
    }

    public BitmapIndexedNode insert1(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1);
        if (node.edit != edit)
            return new BitmapIndexedNode(edit, node.count, node.bitmap1, grow1(node.array, idx, n));
        if (node.array.length >= n + 1) {
            System.arraycopy(node.array, idx, node.array, idx+1, n-idx);
        } else {
            node.array = grow1(node.array, idx, n);
        }
        return node;
    }

    static Object[] grow1(Object[] array, int idx, int n) {
        Object[] newArray = new Object[n + 7]; // room for growth
        System.arraycopy(array, 0, newArray, 0, idx);
        System.arraycopy(array, idx, newArray, idx+1, n-idx);
        return newArray;
    }

    public BitmapIndexedNode insert2(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1);
        if (node.edit != edit)
            return new BitmapIndexedNode(edit, node.count, node.bitmap1, grow2(node.array, idx, n));
        if (node.array.length >= n + 2) {
            System.arraycopy(node.array, idx, node.array, idx+2, n-idx);
        } else {
            node.array = grow2(node.array, idx, n);
        }
        return node;
    }

    static Object[] grow2(Object[] array, int idx, int n) {
        Object[] newArray = new Object[n + 8]; // room for growth
        System.arraycopy(array, 0, newArray, 0, idx);
        System.arraycopy(array, idx, newArray, idx+2, n-idx);
        return newArray;
    }

    public BitmapIndexedNode remove2(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1) - 2;
        if (node.edit != edit)
            return new BitmapIndexedNode(edit, node.count, node.bitmap1, shrink2(node.array, idx, n));
        if (node.array.length > n + 8) {
            node.array = shrink2(node.array, idx, n);
        } else {
            System.arraycopy(node.array, idx+2, node.array, idx, n-idx);
            node.array[n] = node.array[n+1] = null;            
        }
        return node;
    }

    static Object[] shrink2(Object[] array, int idx, int n) {
        Object[] newArray = new Object[n];
        System.arraycopy(array, 0, newArray, 0, idx);
        System.arraycopy(array, idx+2, newArray, idx, n-idx);
        return newArray;
    }

    public BitmapIndexedNode remove1(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1) - 1;
        if (node.edit != edit)
            return new BitmapIndexedNode(edit, node.count, node.bitmap1, shrink1(node.array, idx, n));
        if (node.array.length > n + 8) {
            node.array = shrink1(node.array, idx, n);
        } else {
            System.arraycopy(node.array, idx+1, node.array, idx, n-idx);
            node.array[n] = null;
        }
        return node;
    }

    static Object[] shrink1(Object[] array, int idx, int n) {
        Object[] newArray = new Object[n];
        System.arraycopy(array, 0, newArray, 0, idx);
        System.arraycopy(array, idx+1, newArray, idx, n-idx);
        return newArray;
    }

    public HashCollisionNode edit(HashCollisionNode node) {
        if (node.edit == edit) return node;
        return new HashCollisionNode(edit, node.hash, node.count, node.array.clone());
    }

    public HashCollisionNode insert(HashCollisionNode node) {
        int n = node.count * 2;
        if (node.edit == edit && node.array.length >= n + 2) return node;
        Object[] newArray = new Object[n + 2];
        System.arraycopy(node.array, 0, newArray, 0, n);
        return new HashCollisionNode(edit, node.hash, node.count, newArray);
    }

    public HashCollisionNode remove(HashCollisionNode node, int idx) {
        int n = node.array.length-2;
        if (node.edit != edit) {
            Object[] newArray = new Object[n];
            System.arraycopy(node.array, 0, newArray, 0, idx);
            System.arraycopy(node.array, idx+2, newArray, idx, n-idx);
            return new HashCollisionNode(edit, node.hash, node.count, newArray);
        }
        node.array[idx] = node.array[n];
        node.array[idx+1] = node.array[n+1];
        node.array[n] = node.array[n+1] = null;
        return node;
    }

    public Object[] array(Object a) {
        return new Object[] {a, null, null};
    }

    public Object[] array(Object a, Object b) {
        return new Object[] {a, b, null, null};
    }

    public Object[] array(Object a, Object b, Object c) {
        return new Object[] {a, b, c, null, null};
    }

    public Object[] array(Object a, Object b, Object c, Object d) {
        return new Object[] {a, b, c, d, null, null};
    }
}

final static INodeEditor PERSISTENT_NODE_EDITOR = new INodeEditor() {
    
    public BitmapIndexedNode edit(BitmapIndexedNode node) {
        return new BitmapIndexedNode(null, node.count, node.bitmap1, node.array.clone());
    }

    public BitmapIndexedNode insert1(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1);
        Object[] newArray = new Object[n + 1];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx, newArray, idx+1, n-idx);
        return new BitmapIndexedNode(null, node.count, node.bitmap1, newArray);
    }

    public BitmapIndexedNode insert2(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1);
        Object[] newArray = new Object[n + 2];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx, newArray, idx+2, n-idx);
        return new BitmapIndexedNode(null, node.count, node.bitmap1, newArray);
    }

    public BitmapIndexedNode remove2(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1) - 2;
        Object[] newArray = new Object[n];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx+2, newArray, idx, n-idx);
        return new BitmapIndexedNode(null, node.count, node.bitmap1, newArray);
    }

    public BitmapIndexedNode remove1(BitmapIndexedNode node, int idx) {
        int n = Long.bitCount(node.bitmap1) - 1;
        Object[] newArray = new Object[n];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx+1, newArray, idx, n-idx);
        return new BitmapIndexedNode(null, node.count, node.bitmap1, newArray);
    }

    public HashCollisionNode edit(HashCollisionNode node) {
        return new HashCollisionNode(null, node.hash, node.count, node.array.clone());
    }

    public HashCollisionNode insert(HashCollisionNode node) {
        int n = node.count * 2;
        Object[] newArray = new Object[n + 2];
        System.arraycopy(node.array, 0, newArray, 0, n);
        return new HashCollisionNode(null, node.hash, node.count, newArray);
    }

    public HashCollisionNode remove(HashCollisionNode node, int idx) {
        int n = node.array.length-2;
        Object[] newArray = new Object[n];
        System.arraycopy(node.array, 0, newArray, 0, idx);
        System.arraycopy(node.array, idx+2, newArray, idx, n-idx);
        return new HashCollisionNode(null, node.hash, node.count, newArray);
    }

    public Object[] array(Object a) {
        return new Object[] {a};
    }

    public Object[] array(Object a, Object b) {
        return new Object[] {a, b};
    }

    public Object[] array(Object a, Object b, Object c) {
        return new Object[] {a, b, c};
    }

    public Object[] array(Object a, Object b, Object c, Object d) {
        return new Object[] {a, b, c, d};
    }
};

final static class BitmapIndexedNode implements Serializable {
	static final BitmapIndexedNode EMPTY = new BitmapIndexedNode(null, 0, 0, new Object[0]);

	int count;
	long bitmap1; // each node is a pair of bits, 00 means no node, 01 means a node, 11 means an immediate key value, 10 a collision node  
	Object[] array;
	final AtomicReference<Thread> edit;

	final int index(long bit){
	    return Long.bitCount(bitmap1 & (bit - 1));
	}

	BitmapIndexedNode(AtomicReference<Thread> edit, int count, long bitmap1, Object[] array){
        this.count = count;
	    this.bitmap1 = bitmap1;
		this.array = array;
		this.edit = edit;
	}
	
	public IMapEntry find(int shift, int hash, Object key){
		int bidx = bitsidx(hash, shift);
		long shifted = bitmap1 >>> bidx;
		long type = shifted & 3; 
		if (type == 0) return null;
        if (type == 1) return ((BitmapIndexedNode) array[Long.bitCount(shifted) - 1]).find(shift + 5, hash, key);
		if (type == 3) {
	        int idx = Long.bitCount(shifted) - 2;
	        Object k = array[idx];
	        if(Util.equiv(key, k))
	            return new MapEntry(k, array[idx+1]);
	        return null;
		}
        // type == 2
		return ((HashCollisionNode) array[Long.bitCount(shifted) - 1]).find(hash, key);
	}

	public Object find(int shift, int hash, Object key, Object notFound){
        long bidx = bitsidx(hash, shift);
        long shifted = bitmap1 >>> bidx;
        long type = shifted & 3; 
        if (type == 0) return notFound;
        if (type == 1) return ((BitmapIndexedNode) array[Long.bitCount(shifted) - 1]).find(shift + 5, hash, key, notFound);
        if (type == 3) {
            int idx = Long.bitCount(shifted) - 2;
            Object k = array[idx];
            if(Util.equiv(key, k))
                return array[idx+1];
            return notFound;
        }
        // (type == 2)
        return ((HashCollisionNode) array[Long.bitCount(shifted) - 1]).find(hash, key, notFound);
	}

	public ISeq nodeSeq(){
		return NodeSeq.create(array, bitmap1);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f, bitmap1);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array, f, init, bitmap1);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke(), bitmap1);
	}

	private BitmapIndexedNode editAndSet(INodeEditor editor, int i, Object a) {
		BitmapIndexedNode editable = editor.edit(this);
		editable.array[i] = a;
		return editable;
	}
    
    public BitmapIndexedNode assoc(INodeEditor editor, int shift, int hash, Object key, Object val){
        int bidx = bitsidx(hash, shift);
        long bitmap1 = this.bitmap1;
        long shifted = bitmap1 >>> bidx;
        long type = shifted & 3; 
        int idx = Long.bitCount(shifted);
        if (type == 0) {
            BitmapIndexedNode editable = editor.insert2(this, idx);            
            editable.array[idx] = key;
            editable.array[idx+1] = val;
            editable.count++;
            editable.bitmap1 = bitmap1 | (3L << bidx);
            return editable;
        }
        if (type == 1) {
            --idx;
            BitmapIndexedNode node = (BitmapIndexedNode) array[idx];
            int nd = node.count; // negative delta
            BitmapIndexedNode n = node.assoc(editor, shift + 5, hash, key, val);
            nd -= n.count;
            if (nd == 0 && n == node) return this;
            BitmapIndexedNode editable = editAndSet(editor, idx, n);
            editable.count -= nd;
            return editable;            
        }
        if (type == 3) {
            idx -= 2;
            Object k = array[idx];
            Object v = array[idx+1];
            if(Util.equiv(key, k)) {
                if(val == v) return this;
                BitmapIndexedNode editable = editAndSet(editor, idx+1, val);
                return editable;
            }
            BitmapIndexedNode editable = editor.remove1(this, idx+1);
            editable.count++;
            int hashk = hash(k);
            if (hashk == hash) {
                editable.bitmap1 = bitmap1 ^ (1L << bidx);
                editable.array[idx] = new HashCollisionNode(edit, hash, 2, new Object[] { key, val, k, v });
            } else {
                editable.bitmap1 = bitmap1 ^ (2L << bidx);
                editable.array[idx] = node(editor, shift + 5, hash, key, val, hashk, k, v);                
            }
            return editable;
        }
        // (type == 2)
        --idx;
        HashCollisionNode node = (HashCollisionNode) array[idx];
        if (hash == node.hash) {
            int nd = node.count; // negative delta
            HashCollisionNode n = node.assoc(editor, hash, key, val);
            nd -= n.count;
            if (nd == 0 && n == node) return this; // avoid dummy writes
            BitmapIndexedNode editable = editAndSet(editor, idx, n);
            editable.count -= nd;
            return editable;
        }
        BitmapIndexedNode editable = editAndSet(editor, idx, nest(editor, node, shift + 5, hash, key, val));
        editable.count++;
        editable.bitmap1 = bitmap1 ^ (3L << bidx);
        return editable;            
    }

    private BitmapIndexedNode node(INodeEditor editor, int shift, int h1, Object k1, Object v1, int h2, Object k2, Object v2) {
        // precond: h1 != h2
        int idx1 = bitsidx(h1, shift);
        int idx2 = bitsidx(h2, shift);
        if (idx1 == idx2) return new BitmapIndexedNode(edit, 2, 1L << idx1, editor.array(node(editor, shift+5, h1, k1, v1, h2, k2, v2)));
        return new BitmapIndexedNode(edit, 2, (3L << idx1) | (3L << idx2),
                idx1 > idx2 ? editor.array(k1, v1, k2, v2) : editor.array(k2, v2, k1, v1));
    }
    
	public BitmapIndexedNode without(INodeEditor editor, int shift, int hash, Object key){
        int bidx = bitsidx(hash, shift);
        long bitmap1 = this.bitmap1;
        long shifted = bitmap1 >>> bidx;
        long type = shifted & 3; 
        if (type == 0) return this;
        int idx = Long.bitCount(shifted);
        if (type == 1) {
            idx--;
            BitmapIndexedNode node = (BitmapIndexedNode) array[idx];
            int nd = node.count; // if nd == 2 
            if (nd == 2) return node.withoutOrCollapse(editor, shift + 5, hash, key, this, idx, bidx);
            BitmapIndexedNode n = node.without(editor, shift + 5, hash, key);
            nd -= n.count;
            if (nd == 0) return this;
            BitmapIndexedNode editable = editAndSet(editor, idx, n);
            editable.count -= nd;
            return editable;
        }
        if (type == 3) {
            idx-=2;
            if(!Util.equiv(key, array[idx])) return this;
            BitmapIndexedNode editable = editor.remove2(this, idx);
            editable.bitmap1 = bitmap1 ^ (type << bidx);
            // if there's only one collision node left it should be reparented -- or not, hashcollisionnode may be a not so useful optimization:
            // if there's a near collision (only the last bits differ) a full branch is created, if there's a collision it's optimized.
            editable.count--;
            return editable;
        }
        // type == 2
        idx--;
        HashCollisionNode node = (HashCollisionNode) array[idx];
        int nd = node.count; // if nd == 2 
        if (nd == 2) return node.withoutOrCollapse(editor, hash, key, this, idx, bidx);
        HashCollisionNode n = node.without(editor, hash, key);
        nd -= n.count;
        if (nd == 0) return this;
        BitmapIndexedNode editable = editAndSet(editor, idx, n);
        editable.count -= nd;
        return editable;
	}

	// return the edited parent
    public BitmapIndexedNode withoutOrCollapse(INodeEditor editor, int shift, int hash, Object key, BitmapIndexedNode parent, int pidx, int pbidx) {
        int bidx = bitsidx(hash, shift);
        long bitmap1 = this.bitmap1;
        long shifted = bitmap1 >>> bidx;
        long type = shifted & 3; 
        if (type == 0) return parent;
        int idx = Long.bitCount(shifted);
        if (type == 1) { // nd is necessarily 2 here
            idx--;
            BitmapIndexedNode node = (BitmapIndexedNode) array[idx];
            return node.withoutOrCollapse(editor, shift + 5, hash, key, parent, pidx, pbidx);
        }
        if (type == 3) {
            idx-=2;
            Object k = array[idx];
            if(!Util.equiv(key, k)) return parent;
            return collapse(array, idx, editor, parent, pidx, pbidx);
        }
        // type == 2
        idx--;
        HashCollisionNode node = (HashCollisionNode) array[idx];
        return node.withoutOrCollapse(editor, hash, key, parent, pidx, pbidx);
    } 

//    public INode merge(TransientNodeEditor editor, int shift, INode src) {
//        return src.mergeBitmapNode(editor, shift, this);
//    }
//
//    public INode mergeBitmapNode(TransientNodeEditor editor, int shift, BitmapIndexedNode dst) {
//        int max = Integer.bitCount(this.bitmap | dst.bitmap) + Integer.bitCount(this.kvbitmap | dst.kvbitmap);
//        Object[] newArray = new Object[max];
//        int idx = 0;
//        int srcidx = 0;
//        int dstidx = 0;
//        int newcount = 0;
//        int newbitmap = 0;
//        int newkvbitmap = 0;
//        for(int bit = 1; bit != 0; bit <<= 1) {
//            if ((this.bitmap & bit) != 0) { // src has items
//                newbitmap |= bit;
//                if ((dst.bitmap & bit) != 0) {  // dst has items
//                    if ((this.kvbitmap & bit) != 0) { // src is only one entry
//                        Object key = this.array[srcidx++];
//                        Object val = this.array[srcidx++];
//                        if ((dst.kvbitmap & bit) != 0) { // dst is only one entry
//                            Object dstKey = dst.array[dstidx++];
//                            if (Util.equiv(key, dstKey)) { // src entry overwrites dst entry
//                                newcount++;
//                                newkvbitmap |= bit;
//                                dstidx++;
//                                newArray[idx++] = key;
//                                newArray[idx++] = val;
//                            } else { // keep both entries
//                                newcount+=2;
//                                newArray[idx++] = node(editor, shift+5, hash(key), key, val, hash(dstKey), dstKey, dst.array[dstidx++]);
//                            }
//                        } else { // dst is a node, assoc src entry in it (since src overwrites dst)
//                            INode node = ((INode) dst.array[dstidx++]).assoc(editor, shift+5, hash(key), key, val);
//                            newArray[idx++] = node;
//                            newcount += node.count();
//                        }
//                    } else { // src is a node
//                        INode srcnode = (INode) this.array[srcidx++];
//                        if ((dst.kvbitmap & bit) != 0) { // src node may shadows dst entry
//                            Object key = dst.array[dstidx++];
//                            Object val = dst.array[dstidx++];
//                            int hash = hash(key);
//                            if (srcnode.find(shift+5, hash, key, NOT_FOUND) == NOT_FOUND) {
//                                INode node = srcnode.assoc(editor, shift+5, hash, key, val);
//                                newArray[idx++] = node;
//                                newcount += node.count();
//                            } else {
//                                newArray[idx++] = srcnode;
//                                newcount += srcnode.count();
//                            }
//                        } else { // recursively merge
//                            INode node = PersistentHashMapKV.merge(editor, shift+5, (INode) dst.array[dstidx++], srcnode);
//                            newArray[idx++] = node;
//                            newcount += node.count();
//                        }
//                    }
//                } else { // no dst
//                    if ((this.kvbitmap & bit) != 0) {
//                        newcount++;
//                        newkvbitmap |= bit;
//                        newArray[idx++] = this.array[srcidx++];
//                        newArray[idx++] = this.array[srcidx++];
//                    } else {
//                        INode node = (INode) this.array[srcidx++];
//                        newArray[idx++] = node;
//                        newcount += node.count();
//                    }
//                }
//            } else { // no src
//                if ((dst.bitmap & bit) != 0) {
//                    newcount++;
//                    newbitmap |= bit;
//                    if ((dst.kvbitmap & bit) != 0) {
//                        newkvbitmap |= bit;
//                        newArray[idx++] = dst.array[dstidx++];
//                        newArray[idx++] = dst.array[dstidx++];
//                    } else {
//                        INode node = (INode) dst.array[dstidx++];
//                        newArray[idx++] = node;
//                        newcount += node.count();
//                    }
//                }
//            }
//        }
//        return new BitmapIndexedNode(edit, newcount, newbitmap, newkvbitmap, newArray);
//    }
//
//    public INode mergeCollisionNode(TransientNodeEditor editor, int shift, HashCollisionNode dst) {
//        return mergeCollisionNode(editor, shift, dst, true);
//    }
//
//    INode mergeCollisionNode(TransientNodeEditor editor, int shift, HashCollisionNode dst, boolean srcWin) {
//        long bit = bitpos(dst.hash, shift);
//        int idx = index(bit);
//        if((bitmap & bit) == 0) {
//            BitmapIndexedNode editable = editor.insert1(this, idx);
//            editable.array[idx] = dst;
//            editable.count+=dst.count;
//            editable.bitmap |= bit;
//            return editable;
//        }
//        if((kvbitmap & bit) == 0) {
//            INode node = (INode) array[idx];
//            int ncnt = node.count();
//            int rcnt = count - ncnt;
//            INode n = srcWin ? PersistentHashMapKV.merge(editor, shift+5, dst, node)
//                    : PersistentHashMapKV.merge(editor, shift+5, node, dst);
//            int nncnt = n.count();
//            if(n == node) {
//                if (ncnt != nncnt)
//                    this.count = rcnt + nncnt;
//                return this;
//            }
//            BitmapIndexedNode editable = editAndSet(editor, idx, n);
//            editable.count = rcnt + nncnt;
//            return editable;
//        } 
//        Object k = array[idx];
//        Object v = array[idx+1];
//        int hash = hash(k);
//        BitmapIndexedNode editable = editor.remove1(this, idx+1);
//        editable.kvbitmap ^= bit;
//        INode node = (srcWin || dst.find(shift+5, hash, k, NOT_FOUND) == NOT_FOUND) ? dst.assoc(editor, shift+5, hash, k, v) : dst;
//        editable.array[idx] = node;
//        editable.count += node.count() - 1;
//        return editable;
//    }
}

static BitmapIndexedNode collapse(Object[] array, int idx, INodeEditor editor, BitmapIndexedNode parent, int pidx, int pbidx) {
    // idx can only be 0 or 2, so the other kv pair is just one bitflip away
    idx^=2;
    BitmapIndexedNode editableParent = editor.insert1(parent, pidx);
    editableParent.array[pidx] = array[idx];
    editableParent.array[pidx+1] = array[idx+1];
    editableParent.count--;
    editableParent.bitmap1 |= (3L << pbidx);
    return editableParent;
}

final static class HashCollisionNode implements Serializable {

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

    public IMapEntry find(int hash, Object key){
		int idx = findIndex(key);
		if(idx < 0)
			return null;
		if(Util.equiv(key, array[idx]))
			return new MapEntry(array[idx], array[idx+1]);
		return null;
	}

	public Object find(int hash, Object key, Object notFound){
		int idx = findIndex(key);
		if(idx < 0)
			return notFound;
		if(Util.equiv(key, array[idx]))
			return array[idx+1];
		return notFound;
	}

	public ISeq nodeSeq(){
		return NodeSeq.create(array, (1L << (2 * count)) - 1);
	}

    public Iterator iterator(IFn f){
        return new NodeIter(array, f, (1L << (2 * count)) - 1);
    }

    public Object kvreduce(IFn f, Object init){
         return NodeSeq.kvreduce(array,f,init, (1L << (2 * count)) - 1);
    }

	public Object fold(IFn combinef, IFn reducef, IFn fjtask, IFn fjfork, IFn fjjoin){
		return NodeSeq.kvreduce(array, reducef, combinef.invoke(), (1L << (2 * count)) - 1);
	}

    static int findIndex(Object[] array, Object key, int max){
        for(int i = 0; i < max; i+=2)
            {
            if(Util.equiv(key, array[i]))
                return i;
            }
        return -1;
    }

    int findIndex(Object key){
        return findIndex(array, key, 2*count);
    }

	public HashCollisionNode assoc(INodeEditor editor, int hash, Object key, Object val){
        int idx = findIndex(key);
        if(idx != -1) {
            if(array[idx + 1] == val)
                return this;
            HashCollisionNode editable1 = editor.edit(this);
            editable1.array[idx+1] = val;
            return editable1; 
        }
        
        HashCollisionNode editable = editor.insert(this);
        editable.array[2*count] = key;
        editable.array[2*count+1] = val;
        editable.count++;
        return editable;
	}	

	public HashCollisionNode without(INodeEditor editor, int hash, Object key){
		int idx = findIndex(key);
		if(idx == -1)
			return this;
		HashCollisionNode editable = editor.remove(this, idx);
		editable.count--;
		return editable;
	}

    public BitmapIndexedNode withoutOrCollapse(INodeEditor editor, int hash,
            Object key, BitmapIndexedNode parent, int pidx, int pbidx) {
        int idx = findIndex(key);
        if(idx == -1)
            return parent;
        return collapse(array, idx, editor, parent, pidx, pbidx);
    }

//    public INode merge(TransientNodeEditor editor, int shift, INode src) {
//        return src.mergeCollisionNode(editor, shift, this);
//    }

//    public INode mergeBitmapNode(TransientNodeEditor editor, int shift, BitmapIndexedNode dst) {
//        return dst.mergeCollisionNode(editor, shift, this, false);
//    }

//    public INode mergeCollisionNode(TransientNodeEditor editor, int shift, HashCollisionNode dst) {
//        if (this.hash != dst.hash) return nest(editor, shift, dst);
//        Object[] newArray = new Object[(this.count+dst.count)*2];
//        int max = dst.count*2;
//        int newCount = dst.count;
//        System.arraycopy(dst.array, 0, newArray, 0, max);
//        for(int i = 0; i < this.count*2; i+=2) {
//            int idx = findIndex(newArray, this.array[i], max);
//            if (idx < 0) {
//                newArray[newCount*2] = this.array[i]; 
//                newArray[newCount*2+1] = this.array[i+1];
//                newCount++;
//                continue;
//            }
//            if (Util.equiv(this.array[i+1], newArray[idx+1])) continue;
//            newArray[idx+1] = this.array[i+1];
//        }
//        return new HashCollisionNode(edit, hash, newCount, newArray);
//    }
    
//    static BitmapIndexedNode nest(INodeEditor editor, HashCollisionNode collisionNode, int shift, HashCollisionNode node) {
//        long bit = bitpos(node.hash, shift);
//        int bitthis = bitpos(hash, shift);
//        if (bit == bitthis) return new BitmapIndexedNode(edit, count+node.count, bit, 0, editor.array(nest(editor, shift+5, node)));
//        return new BitmapIndexedNode(edit, count+node.count, bit | bitthis,  bit, (bit - bitthis) < 0 ? editor.array(node, this) : editor.array(this, node));
//    }


}

static BitmapIndexedNode nest(INodeEditor editor, HashCollisionNode collisionNode, int shift, int h, Object k, Object v) {
    int bidx = bitsidx(h, shift);
    int cbidx = bitsidx(collisionNode.hash, shift);
    if (bidx == cbidx) {
        BitmapIndexedNode node = nest(editor, collisionNode, shift+5, h, k, v);
        return new BitmapIndexedNode(node.edit, node.count, 1L << bidx, editor.array(node));
    }
    return new BitmapIndexedNode(collisionNode.edit /* TODO use the editor */, 
            collisionNode.count+1, (3L << bidx) | (2L << cbidx) ,  bidx > cbidx ? editor.array(k, v, collisionNode) : editor.array(collisionNode, k, v));
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
		IPersistentMap map = PersistentHashMapKV.EMPTY;
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

static void assertCoherent1(long bitmap1, Object[] array) {
    try {
   int i = Long.bitCount(bitmap1) - 1;
   for (;bitmap1 != 0; bitmap1 >>>= 2) {
       switch ((int)(bitmap1 & 3)) {
       case 0: continue;
       case 1: if (!(array[i] instanceof BitmapIndexedNode)) throw new RuntimeException();
           i--; break;
       case 2: if (!(array[i] instanceof HashCollisionNode)) throw new RuntimeException();
           i--; break;
       case 3: i-=2; break;
       };
   }
   if (i > 0) throw new RuntimeException();
    } catch (RuntimeException e) {
        System.err.println(Long.toBinaryString(bitmap1) + " " + RT.seq(array));
        throw e;
    }
}

static void assertCoherent(BitmapIndexedNode node) {
    long bitmap1 = node.bitmap1;
    Object[] array = node.array;
    try {
   int i = Long.bitCount(bitmap1) - 1;
   for (;bitmap1 != 0; bitmap1 >>>= 2) {
       switch ((int)(bitmap1 & 3)) {
       case 0: continue;
       case 1: if (!(array[i] instanceof BitmapIndexedNode)) throw new RuntimeException();
           assertCoherent((BitmapIndexedNode) array[i]);
           i--; break;
       case 2: if (!(array[i] instanceof HashCollisionNode)) throw new RuntimeException();
           i--; break;
       case 3: i-=2; break;
       };
   }
   if (i > 0) throw new RuntimeException();
    } catch (RuntimeException e) {
        System.err.println(Long.toBinaryString(bitmap1) + " " + RT.seq(array));
        throw e;
    }
}

private static int bitsidx(int hash, int shift){
	return mask(hash, shift) * 2;
}

static final class NodeIter implements Iterator {
    private static final Object NULL = new Object();
    final Object[] array;
    final IFn f;
    private int i;
    private Object nextEntry = NULL;
    private Iterator nextIter;
    private long bitmap1;

    NodeIter(Object[] array, IFn f, long bitmap1){
        this.array = array;
        this.f = f;
        this.bitmap1 = bitmap1;
        this.i = Long.bitCount(bitmap1)-1;
    }

    private boolean advance(){
        for (;bitmap1 != 0; bitmap1 >>>= 2)
        {
            long type = bitmap1 & 3;
            if (type == 0) continue;
            if (type == 3)
            {
                Object key = array[i-1];
                Object val = array[i];
                i -= 2;
                bitmap1 >>>= 2;
                nextEntry = f.invoke(key, val);
                return true;
            }
            Iterator iter = (type == 1) ? ((BitmapIndexedNode) array[i]).iterator(f) : ((HashCollisionNode) array[i]).iterator(f);
            i-=1;
            if(iter != null && iter.hasNext())
            {
                bitmap1 >>>= 2;
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
	final long bitmap1;
	final int i;
	final ISeq s;
	
	NodeSeq(Object[] array, int i, long bitmap1) {
		this(null, array, i, null, bitmap1);
	}

	static ISeq create(Object[] array, long bitmap1) {
		return create(array, Long.bitCount(bitmap1)-1, null, bitmap1);
	}

    static public Object kvreduce(Object[] array, IFn f, Object init, long bitmap1){
        int i = Long.bitCount(bitmap1) - 1;
        for(; bitmap1 !=0; bitmap1 >>>= 2) {
            if ((bitmap1 & 3) == 0) continue;
            if ((bitmap1 & 3) == 3) {
                init = f.invoke(init, array[i-1], array[i]);
                i-=2;
            } else {
                init = ((BitmapIndexedNode) array[i]).kvreduce(f,init);
                i--;
            }
            if(RT.isReduced(init))
                return init;
        }
        return init;
    }

	private static ISeq create(Object[] array, int i, ISeq s, long bitmap1) {
		if(s != null)
			return new NodeSeq(null, array, i, s, bitmap1);
		for(; bitmap1 != 0; bitmap1 >>>= 2) {
		    long type = bitmap1 & 3;
            if (type == 0) continue;
			if (type == 3)
				return new NodeSeq(null, array, i, null, bitmap1 >>> 2);
			ISeq nodeSeq = (type == 1) ? ((BitmapIndexedNode) array[i]).nodeSeq() : ((HashCollisionNode) array[i]).nodeSeq();
			if(nodeSeq != null)
			    return new NodeSeq(null, array, i-1, nodeSeq, bitmap1 >>> 2);
			i--; // empty seq for child node, should not happen
		}
		return null;
	}
	
	NodeSeq(IPersistentMap meta, Object[] array, int i, ISeq s, long bitmap1) {
		super(meta);
		this.bitmap1 = bitmap1;
		this.array = array;
		this.i = i;
		this.s = s;
	}

	public Obj withMeta(IPersistentMap meta) {
		return new NodeSeq(meta, array, i, s, bitmap1);
	}

	public Object first() {
		if(s != null)
			return s.first();
		return new MapEntry(array[i-1], array[i]);
	}

	public ISeq next() {
		if(s != null)
			return create(array, i, s.next(), bitmap1);
		return create(array, i - 2, null, bitmap1);
	}
}

}

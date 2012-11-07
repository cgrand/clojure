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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple implementation of persistent map on an array
 * <p/>
 * Note that instances of this class are constant values
 * i.e. add/remove etc return new values
 * <p/>
 * Copies array on every change, so only appropriate for _very_small_ maps
 * <p/>
 * null keys and values are ok, but you won't be able to distinguish a null value via valAt - use contains/entryAt
 */

public class PersistentBitmapMap extends APersistentMap implements IObj, IEditableCollection,
																	IKeywordLookup {
static final int HASHTABLE_THRESHOLD = 16;

final Object[] array;
final long bitmap;
final IPersistentMap _meta;

public static final PersistentBitmapMap EMPTY = new PersistentBitmapMap();

static public IPersistentMap create(Map other){
	ITransientMap ret = EMPTY.asTransient();
	for(Object o : other.entrySet())
		{
		Map.Entry e = (Entry) o;
		ret = ret.assoc(e.getKey(), e.getValue());
		}
	return ret.persistent();
}

protected PersistentBitmapMap(){
	this.array = new Object[0];
	this._meta = null;
	this.bitmap = 0;
}

public PersistentBitmapMap withMeta(IPersistentMap meta){
	return new PersistentBitmapMap(meta, array, bitmap);
}

PersistentBitmapMap create(Object[] init, long bitmap){
	return new PersistentBitmapMap(meta(), init, bitmap);
}

IPersistentMap createHT(Object[] init){
	return PersistentHashMap.create(meta(), init);
}

static public IPersistentMap createWithCheck(Object[] init){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < init.length; i += 2)
		{
		ret = ret.assoc(init[i], init[i + 1]);
		if(ret.count() != i/2 + 1)
			throw new IllegalArgumentException("Duplicate key: " + init[i]);
		}
	return ret.persistent();
}

static public IPersistentMap create(ISeq items){
	ITransientMap ret = EMPTY.asTransient();
	for(; items != null; items = items.next().next())
		{
		if(items.next() == null)
			throw new IllegalArgumentException(String.format("No value supplied for key: %s", items.first()));
		ret = ret.assoc(items.first(), RT.second(items));
		}
	return ret.persistent();
}

static public IPersistentMap create(Object[] items){
	ITransientMap ret = EMPTY.asTransient();
	for(int i = 0; i < items.length; i += 2)
		{
		ret = ret.assoc(items[i], items[i+1]);
		}
	return ret.persistent();
}

/**
 * This ctor captures/aliases the passed array, so do not modify later
 *
 * @param init {key1,val1,key2,val2,...}
 */
public PersistentBitmapMap(Object[] init, long bitmap){
	this.array = init;
	this.bitmap = bitmap;
	this._meta = null;
}


public PersistentBitmapMap(IPersistentMap meta, Object[] init, long bitmap){
	this._meta = meta;
	this.array = init;
	this.bitmap = bitmap;
}

public int count(){
	return array.length / 2;
}

static int hash(Object key) {
	return Util.hasheq(key);
}

static int index(long bitmap, long bitmask) {
	return Long.bitCount(bitmap & (bitmask - 1)) * 2;
}

static long bitmask1(int hash) {
	return 1L << (hash & 0x3f);
}

static long bitmask2(int hash) {
	return 1L << ((hash >>> 6) & 0x3f);
}

public boolean containsKey(Object key){
	return indexOf(key) >= 0;
}

public IMapEntry entryAt(Object key){
	int i = indexOf(key);
	if(i >= 0)
		return new MapEntry(array[i],array[i+1]);
	return null;
}

public IPersistentMap assocEx(Object key, Object val) {
	long m = assocExMaskOf(key);
	if(m == 0)
		if (indexOf(key) >= 0)
			throw Util.runtimeException("Key already present");
		else // double conflict, convert to hashmap
			return createHT(array).assocEx(key, val);
	//didn't have key, grow
	if(array.length >= HASHTABLE_THRESHOLD)
		return createHT(array).assocEx(key, val);
	Object[] newArray = new Object[array.length + 2];
	int i = index(bitmap, m);
	if(array.length > 0) {
		System.arraycopy(array, 0, newArray, 0, i);
		System.arraycopy(array, i, newArray, i+2, array.length - i);
	}
	newArray[i] = key;
	newArray[i + 1] = val;
	return create(newArray, bitmap | m);
}

public IPersistentMap assoc(Object key, Object val){
	Object[] newArray;
	long m = assocMaskOf(key);
	if (m == 0) // double conflict, convert to hashmap
		return createHT(array).assocEx(key, val);
	if((m & bitmap) != 0) {//already have key
		int i = index(bitmap, m);
		if(array[i + 1] == val) //no change, no op
			return this;
		newArray = array.clone();
		newArray[i + 1] = val;
		return create(newArray, bitmap);
	}
	//didn't have key, grow
	if(array.length >= HASHTABLE_THRESHOLD)
		return createHT(array).assoc(key, val);
	int i = index(bitmap, m);
	newArray = new Object[array.length + 2];
	if(array.length > 0) {
		System.arraycopy(array, 0, newArray, 0, i);
		System.arraycopy(array, i, newArray, i+2, array.length - i);
	}
	newArray[i] = key;
	newArray[i + 1] = val;
	return create(newArray, bitmap | m);
}

public IPersistentMap without(Object key){
	// TOOO evaluate fastIndexOf + bit twiddling to find the bit by its rank
	long m = dissocMaskOf(key);
	if((m & bitmap) != 0) //have key, will remove
		{
		int newlen = array.length - 2;
		if(newlen == 0)
			return empty();
		Object[] newArray = new Object[newlen];
		int i = index(bitmap, m);
		System.arraycopy(array, 0, newArray, 0, i);
		System.arraycopy(array, i + 2, newArray, i, newArray.length - i);
		return create(newArray, bitmap ^ m);
		}
	//don't have key, no op
	return this;
}

public IPersistentMap empty(){
	return (IPersistentMap) EMPTY.withMeta(meta());
}

final public Object valAt(Object key, Object notFound){
	int i = indexOf(key);
	if(i >= 0)
		return array[i + 1];
	return notFound;
}

public Object valAt(Object key){
	return valAt(key, null);
}

public int capacity(){
	return count();
}

private int indexOf(Object key){
	int i;
	int h = hash(key);
	long mask = bitmask1(h);
	if ((bitmap & mask) != 0) {
		i = index(bitmap, mask);
		if (Util.equiv(array[i], key)) return i;
	}
	mask = bitmask2(h);
	if ((bitmap & mask) != 0) {
		i = index(bitmap, mask);
		if (Util.equiv(array[i], key)) return i;
	}
	return -1;
}

private long dissocMaskOf(Object key){
	int h = hash(key);
	long mask1 = bitmask1(h);
	Object k1;
	if ((bitmap & mask1) != 0) {
		k1 = array[index(bitmap, mask1)];
		if (k1 == key) return mask1;
	} else {
		k1 = this;
	}
	long mask2 = bitmask2(h);
	Object k2;
	if ((bitmap & mask2) != 0) {
		k2 = array[index(bitmap, mask2)];
		if (key == k2) return mask2;
	} else {
		k2 = this;
	}
	if ((k1 != this) && (Util.equiv(k1, key))) return mask1;
	if ((k2 != this) && (Util.equiv(k2, key))) return mask2;
	return 0;
}

public long assocMaskOf(Object key){
	int h = hash(key);
	long mask1 = bitmask1(h);
	if (((bitmap & mask1) != 0) && Util.equiv(array[index(bitmap, mask1)], key)) 
		return mask1;
	long mask2 = bitmask2(h);
	if (((bitmap & mask2) != 0) && Util.equiv(array[index(bitmap, mask2)], key)) 
		return mask2;
	if ((bitmap & mask1) == 0) return mask1;
	if ((bitmap & mask2) == 0) return mask2;
	return 0;
}

private long assocExMaskOf(Object key){
	int h = hash(key);
	long mask1 = bitmask1(h);
	if ((bitmap & mask1) == 0) return mask1;
	long mask2 = bitmask2(h);
	if ((bitmap & mask2) == 0) return mask2;
	return 0;
}

static boolean equalKey(Object k1, Object k2){
	return Util.equiv(k1, k2);
}

public Iterator iterator(){
	return new Iter(array);
}

public ISeq seq(){
	if(array.length > 0)
		return new Seq(array, 0);
	return null;
}

public IPersistentMap meta(){
	return _meta;
}

static class Seq extends ASeq implements Counted{
	final Object[] array;
	final int i;

	Seq(Object[] array, int i){
		this.array = array;
		this.i = i;
	}

	public Seq(IPersistentMap meta, Object[] array, int i){
		super(meta);
		this.array = array;
		this.i = i;
	}

	public Object first(){
		return new MapEntry(array[i],array[i+1]);
	}

	public ISeq next(){
		if(i + 2 < array.length)
			return new Seq(array, i + 2);
		return null;
	}

	public int count(){
		return (array.length - i) / 2;
	}

	public Obj withMeta(IPersistentMap meta){
		return new Seq(meta, array, i);
	}
}

static class Iter implements Iterator{
	Object[] array;
	int i;

	//for iterator
	Iter(Object[] array){
		this(array, -2);
	}

	//for entryAt
	Iter(Object[] array, int i){
		this.array = array;
		this.i = i;
	}

	public boolean hasNext(){
		return i < array.length - 2;
	}

	public Object next(){
		i += 2;
		return new MapEntry(array[i],array[i+1]);
	}

	public void remove(){
		throw new UnsupportedOperationException();
	}

}

public Object kvreduce(IFn f, Object init){
    for(int i=0;i < array.length;i+=2){
        init = f.invoke(init, array[i], array[i+1]);
	    if(RT.isReduced(init))
		    return ((IDeref)init).deref();
        }
    return init;
}

public ITransientMap asTransient(){
	return new TransientBitmapMap(array, bitmap);
}

static final class TransientBitmapMap extends ATransientMap implements IKeywordLookup {
	int len;
	long bitmap;
	final Object[] array;
	Thread owner;

	public TransientBitmapMap(Object[] array, long bitmap){
		this.owner = Thread.currentThread();
		this.array = new Object[Math.max(HASHTABLE_THRESHOLD, array.length)];
		System.arraycopy(array, 0, this.array, 0, array.length);
		this.len = array.length;
		this.bitmap = bitmap;
	}
	
	private int indexOf(Object key){
		int i;
		int h = hash(key);
		long mask = bitmask1(h);
		if ((bitmap & mask) != 0) {
			i = index(bitmap, mask);
			if (Util.equiv(array[i], key)) return i;
		}
		mask = bitmask2(h);
		if ((bitmap & mask) != 0) {
			i = index(bitmap, mask);
			if (Util.equiv(array[i], key)) return i;
		}
		return -1;
	}

	private long dissocMaskOf(Object key){
		int h = hash(key);
		long mask1 = bitmask1(h);
		int idx1;
		Object k1;
		if ((bitmap & mask1) != 0) {
			idx1 = index(bitmap, mask1);
			k1 = array[idx1];
			if (k1 == key) return mask1;
		} else {
			idx1 = -1;
			k1 = null;
		}
		long mask2 = bitmask2(h);
		int idx2;
		Object k2;
		if ((bitmap & mask2) != 0) {
			idx2 = index(bitmap, mask2);
			k2 = array[idx2];
			if (key == k2) return mask2;
		} else {
			idx2 = -1;
			k2 = null;
		}
		if ((idx1 >= 0) && (Util.equiv(k1, key))) return mask1;
		if ((idx2 >= 0) && (Util.equiv(k2, key))) return mask1;
		return 0;
	}

	private long assocMaskOf(Object key){
		int h = hash(key);
		long mask1 = bitmask1(h);
		if (((bitmap & mask1) != 0) && Util.equiv(array[index(bitmap, mask1)], key)) 
			return mask1;
		long mask2 = bitmask2(h);
		if (((bitmap & mask2) != 0) && Util.equiv(array[index(bitmap, mask2)], key)) 
			return mask2;
		if ((bitmap & mask1) == 0) return mask1;
		if ((bitmap & mask2) == 0) return mask2;
		return 0;
	}

	ITransientMap doAssoc(Object key, Object val){
		long m = assocMaskOf(key);
		if (m == 0) // double conflict, convert to hashmap
			return PersistentHashMap.create(array, len).asTransient().assoc(key, val);
		if((m & bitmap) != 0) { //already have key, same-sized replacement
			int i = index(bitmap, m);
			if(array[i + 1] != val) //no change, no op
				array[i + 1] = val;
			return this;
		}
		//didn't have key, grow
		if(len >= array.length)
			return PersistentHashMap.create(array).asTransient().assoc(key, val);
		int i = index(bitmap, m);
		System.arraycopy(array, i, array, i+2, len - i);
		array[i] = key;
		array[i + 1] = val;
		len += 2;
		bitmap |= m; 
		return this;
	}

	ITransientMap doWithout(Object key) {
		long m = dissocMaskOf(key);
		if((m & bitmap) != 0) //have key, will remove
			{
			int i = index(bitmap, m);
			System.arraycopy(array, i + 2, array, i, len - i - 2);
			len -= 2;
			bitmap ^= m;
			}
		//don't have key, no op
		return this;
	}

	Object doValAt(Object key, Object notFound) {
		int i = indexOf(key);
		if (i >= 0)
			return array[i + 1];
		return notFound;
	}

	int doCount() {
		return len / 2;
	}
	
	IPersistentMap doPersistent(){
		ensureEditable();
		owner = null;
		Object[] a = new Object[len];
		System.arraycopy(array,0,a,0,len);
		return new PersistentBitmapMap(a, bitmap);
	}

	void ensureEditable(){
		if(owner == Thread.currentThread())
			return;
		if(owner != null)
			throw new IllegalAccessError("Transient used by non-owner thread");
		throw new IllegalAccessError("Transient used after persistent! call");
	}

	public ILookupThunk getLookupThunk(final Keyword key) {
		int h = hash(key);
		final long mask1 = bitmask1(h);
		final long mask2 = bitmask2(h);
		
		return new ILookupThunk() {
			
			public Object get(Object target) {
				if (target instanceof TransientBitmapMap) {
					TransientBitmapMap m = (TransientBitmapMap) target;
					Object k1;
					int idx1;
					if ((m.bitmap & mask1) != 0) {
						idx1 = index(m.bitmap, mask1);
						k1 = m.array[idx1];
						if (k1 == key) return m.array[idx1 + 1];
					} else {
						idx1 = -1;
						k1 = null;
					}
					Object k2;
					int idx2;
					if ((m.bitmap & mask2) != 0) {
						idx2 = index(m.bitmap, mask2);
						k2 = m.array[idx2];
						if (key == k2) return m.array[idx2 + 1];
					} else {
						idx2 = -1;
						k2 = null;
					}
					if ((idx1 >= 0) && (Util.equiv(k1, key))) return m.array[idx1 + 1];
					if ((idx2 >= 0) && (Util.equiv(k2, key))) return m.array[idx2 + 1];
					return null;
				}
				return this;
			}
		};
	}}

public ILookupThunk getLookupThunk(final Keyword key) {
	int h = hash(key);
	final long mask1 = bitmask1(h);
	final long mask2 = bitmask2(h);
	
	return new ILookupThunk() {
		
		public Object get(Object target) {
			if (target instanceof PersistentBitmapMap) {
				PersistentBitmapMap m = (PersistentBitmapMap) target;
				Object k1;
				int idx1;
				if ((m.bitmap & mask1) != 0) {
					idx1 = index(m.bitmap, mask1);
					k1 = m.array[idx1];
					if (k1 == key) return m.array[idx1 + 1];
				} else {
					idx1 = -1;
					k1 = null;
				}
				Object k2;
				int idx2;
				if ((m.bitmap & mask2) != 0) {
					idx2 = index(m.bitmap, mask2);
					k2 = m.array[idx2];
					if (key == k2) return m.array[idx2 + 1];
				} else {
					idx2 = -1;
					k2 = null;
				}
				if ((idx1 >= 0) && (Util.equiv(key, k1))) return m.array[idx1 + 1];
				if ((idx2 >= 0) && (Util.equiv(key, k2))) return m.array[idx2 + 1];
				return null;
			}
			return this;
		}
	};
}
}

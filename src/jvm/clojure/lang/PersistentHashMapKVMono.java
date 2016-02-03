package clojure.lang;

import java.util.Iterator;

public class PersistentHashMapKVMono extends APersistentMap {
    static public /**/ final class Node {
        public /**/ long bitmap;
        public /**/ Object array[];
        public /**/ int count;

        Node(long bitmap, Object[] array, int count) {
            this.bitmap = bitmap;
            this.array = array;
            this.count = count;
        }
        
        static final Node EMPTY = new Node(0L, new Object[0], 0); 
    }
    
    static public /**/ final class Collisions {
        int count;
        int hash;
        Object array[];
        
        public Collisions(int hash, Object[] array, int count) {
            this.count = count;
            this.hash = hash;
            this.array = array;
        }
    }
    
    public /**/ Node root;
    
    public PersistentHashMapKVMono(Node root) {
        this.root = root;
    }
    
    public static final PersistentHashMapKVMono EMPTY = new PersistentHashMapKVMono(Node.EMPTY);

    static Node assoc(Node node, Object key, Object val, int hash, int lvl) {
        int shift = ((hash >>> lvl) & 31)*2;
        long lead = node.bitmap >>> shift;
        int bits = (int) (lead & 3L);
        int pos = Long.bitCount(lead);
        switch (bits) {
        case 0:
            return copyAndInsert(node, shift, pos, key, val);
        case 1:
            Node child = (Node) node.array[pos-1];
            Node newchild = assoc(child, key, val, hash, lvl+5);
            if (newchild == child) return node;
            return copyAndSet(node, pos-1, newchild, node.count+1);
        case 2:
            Collisions collisions = (Collisions) node.array[pos-1];
            Object newcollisions = extendCollision(collisions, key, val);
            if (newcollisions == collisions) return node;
            return copyAndSet(node, pos-1, newcollisions, node.count+1);
        default:
            Object k = node.array[pos-2];
            Object v = node.array[pos-1];
            if (Util.equiv(key, k))
                return val != v ? copyAndSet(node, pos-1, v, node.count) : node;
            int h = Util.hasheq(k);
            if (h == hash)
                return copyAndPromote(node, 1L << shift, pos, new Collisions(hash, new Object[] {k, v, key, val}, 2));
            return copyAndPromote(node, 2L << shift, pos, pushdown(lvl+5, hash, key, val, h, k, v));
        }
    }
    
    private static Collisions extendCollision(Collisions collisions,
            Object key, Object val) {
        int len = 2*collisions.count;
        for(int i = len-2; i >= 0; i-=2) {
            if (collisions.array[i] == key) 
                if (collisions.array[i+1] == val)
                    return collisions;
                return new Collisions(collisions.hash, aset(collisions.array, i+1, val), collisions.count);
        }
        int count = collisions.count + 1;
        Object[] array = new Object[2*count];
        System.arraycopy(collisions.array, 0, array, 0, len);
        array[len] = key;
        array[len+1] = val;
        return new Collisions(collisions.hash, array, count);
    }

    private static Node pushdown(int lvl, int hash, Object key, Object val,
            int h, Object k, Object v) {
        int idx = (hash >>> lvl) & 31;
        int i = (h >>> lvl) & 31;
        if (i == idx) return new Node(1L << (2 * idx), new Object[] { pushdown(lvl+5, hash, key, val, h, k, v) }, 2); // could be a loop
        return new Node(3L << (2 * idx) | 3L << (2 * i), idx < i ? new Object[] { k, v, key, val } : new Object[] { key, val, k, v }, 2);
    }

    private static Node copyAndPromote(Node node, long mask, int pos, Object x) {
        long bitmap = node.bitmap ^ mask;
        int len = Long.bitCount(bitmap);
        Object[] array = new Object[len];
        System.arraycopy(node.array, 0, array, 0, pos-2);
        array[pos-2] = x;
        System.arraycopy(node.array, pos, array, pos-1, len-pos+1);
        return new Node(bitmap, array, node.count+1);
    }

    private static Node copyAndSet(Node node, int i, Object v, int count) {
        return new Node(node.bitmap, aset(node.array, i, v), count);
    }

    private static Object[] aset(Object[] array, int i, Object v) {
        Object[] a = array.clone();
        a[i] = v;
        return a;
    }

    static Node copyAndInsert(Node node, int shift, int pos, Object key, Object val) {
        long bitmap = node.bitmap | (3L << shift);
        int len = Long.bitCount(bitmap);
        Object[] array = new Object[len];
        System.arraycopy(node.array, 0, array, 0, pos);
        array[pos] = key;
        array[pos+1] = val;
        System.arraycopy(node.array, pos, array, pos+2, len-pos-2);
        return new Node(bitmap, array, node.count+1);
    }

    static Object lookupCollision(Object collisions_, int hash, Object key, Object notFound) {
        Collisions collisions = (Collisions) collisions_;
        if (collisions.hash != hash) return notFound;
        for(int i = 2*(collisions.count - 1); i >= 0; i-=2) {
            if (collisions.array[i] == key) return collisions.array[i+1];
        }
        return notFound;
    }

    public IPersistentMap assoc(Object key, Object val) {
        Node newroot = assoc(root, key, val, Util.hasheq(key), 0);
        if (newroot == root) return this;
        return new PersistentHashMapKVMono(newroot);
    }    

    public IPersistentMap assocEx(Object key, Object val) {
        Node newroot = assoc(root, key, val, Util.hasheq(key), 0);
        if (newroot.count == root.count)
            throw Util.runtimeException("Key already present");
        return new PersistentHashMapKVMono(newroot);
    }

    public IPersistentMap without(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public Iterator iterator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean containsKey(Object key) {
        return valAt(key, root) != root;
    }

    public IMapEntry entryAt(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public int count() {
        return root.count;
    }

    public IPersistentCollection empty() {
        // TODO support meta
        return new PersistentHashMapKVMono(Node.EMPTY);
    }

    public ISeq seq() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public Object valAt(Object key) {
        return valAt(key, null);
    }

    public Object valAt(Object key, Object notFound) {
        int hash = Util.hasheq(key);
        int h = hash;
        Node node = root;
        loop: for (;;) {
            int shift = (h & 31)*2;
            long lead = node.bitmap >>> shift;
            int pos = Long.bitCount(lead);
            int bits = (int) (lead & 3L);
            switch (bits) {
            case 0:
                return notFound;
            case 1:
                h >>>= 5;
                node = (Node) node.array[pos-1];
                continue loop;
            case 2:
                return lookupCollision(node.array[pos-1], hash, key, notFound);
            default:
                if (Util.equiv(key, node.array[pos-2])) return node.array[pos-1];
                return notFound;
            }
        }
    }
}

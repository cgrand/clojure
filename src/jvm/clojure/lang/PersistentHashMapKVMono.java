package clojure.lang;

import java.util.Iterator;

public class PersistentHashMapKVMono extends APersistentMap {
    static private final class Node {
        static class Seq extends ASeq {
            final long bitmap;
            final Object[] array;
            final ISeq nexts;
            final int lvl;
            
            private static ISeq create(long bitmap, Object[] array, ISeq nexts, int lvl) {
                while (bitmap != 0L) {
                    switch ((int) (bitmap & 3L)) {
                    case 0: 
                        bitmap >>>= Long.numberOfTrailingZeros(bitmap) & -2;
                        break;
                    case 3:
                        return new Seq(null, bitmap >>> 2, array, nexts, lvl);
                    default:
                        Object object = array[Long.bitCount(bitmap) - 1];
                        nexts = lvl == 30 ? Collisions.Seq.create(object, nexts) : Node.Seq.create(object, nexts, lvl+5);
                        bitmap >>>= 2;
                    }
                }
                return nexts;
            }

            static ISeq create(Object node_, ISeq nexts, int lvl) {
                Node child = (Node) node_;
                return create(child.bitmap, child.array, nexts, lvl);
            }
            
            private Seq(IPersistentMap meta, long bitmap, Object[] array, ISeq nexts, int lvl) {
                super(meta);
                this.bitmap = bitmap;
                this.array = array;
                this.nexts = nexts;
                this.lvl = lvl;
            }
        
            public Object first() {
                int pos = Long.bitCount(bitmap);
                return new MapEntry(array[pos], array[pos+1]);
            }
        
            public ISeq next() {
                return create(bitmap, array, nexts, lvl);
            }
        
            public Obj withMeta(IPersistentMap meta) {
                return new Seq(meta, bitmap, array, nexts, lvl);
            }
            
        }

        private long bitmap;
        private Object array[];
        private int count;

        Node(long bitmap, Object[] array, int count) {
            this.bitmap = bitmap;
            this.array = array;
            this.count = count;
        }
        
        static final Node EMPTY = new Node(0L, new Object[0], 0);

        static Node assoc(Node node, int hash, Object key, Object val, int lvl) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return Node.copyAndInsert(node, shift, pos, key, val);
            case 3:
                Object k = node.array[pos-2];
                Object v = node.array[pos-1];
                if (Util.equiv(key, k))
                    return val != v ? Node.copyAndSet(node, pos-1, val, node.count) : node;
                return Node.copyAndPromote(node, shift, pos, pushdown(lvl+5, hash, key, val, Util.hasheq(k), k, v));
            default:
                Object child = node.array[pos-1];
                if (lvl == 30) {
                    Collisions collisions = (Collisions) child;
                    Collisions newcollisions = Collisions.extend(collisions, key, val);
                    if (newcollisions == collisions) return node;
                    return Node.copyAndSet(node, pos-1, newcollisions, node.count+newcollisions.count-collisions.count);
                } else {
                    Node childnode = (Node) child;
                    Node newchild = assoc(childnode, hash, key, val, lvl+5);
                    if (childnode == child) return node;
                    return Node.copyAndSet(node, pos-1, newchild, node.count+newchild.count-childnode.count);                    
                }
            }
        }

        static Node dissoc(Node node, int hash, Object key, int lvl) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return node;
            case 3:
                Object k = node.array[pos-2];
                if (Util.equiv(key, k))
                    return copyAndRemoveKV(node, shift, pos-2);
                return node;
            default:
                Object child = node.array[pos-1];
                return dissocMayCollapse((Node) child, hash, key, lvl+5, node, shift, pos);
            }
        }

        static Node copyAndDemote(Node node, int shift, int pos, Object k, Object v) {
            long bitmap = node.bitmap | (3L << shift); // set to 3
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos);
            array[pos] = k;
            array[pos+1] = v;
            System.arraycopy(node.array, pos+1, array, pos+2, len-(pos+2));
            return new Node(bitmap, array, node.count-1);
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

        static Node copyAndPromote(Node node, int shift, int pos, Object x) {
            long bitmap = node.bitmap ^ (1L << shift); // set to 2
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos-2);
            array[pos-2] = x;
            System.arraycopy(node.array, pos, array, pos-1, len-pos+1);
            return new Node(bitmap, array, node.count+1);
        }

        static Node copyAndRemoveKV(Node node, int shift, int pos) {
            long bitmap = node.bitmap ^ (3L << shift); // set to 2
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos);
            System.arraycopy(node.array, pos+2, array, pos, len-pos);
            return new Node(bitmap, array, node.count-1);
        }

        static Node copyAndSet(Node node, int i, Object v, int count) {
            return new Node(node.bitmap, aset(node.array, i, v), count);
        }

        static Node dissocMayCollapse(Node node, int hash, Object key, int lvl, Node parent, int pshift, int ppos) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return parent;
            case 3:
                Object k = node.array[pos-2];
                if (Util.equiv(key, k))
                    if (node.count > 2)
                        return copyAndSet(parent, ppos-1, copyAndRemoveKV(node, shift, pos-2), parent.count-1);
                    else {
                        pos = (pos - 2)^2;
                        return copyAndDemote(parent, pshift, ppos-1, node.array[pos], node.array[pos+1]);                    
                    }
                return parent;
            default:
                Object child = node.array[pos-1];
                if (node.count > 2) {
                    Node newnode = lvl == 30 ? Collisions.dissoc(node, shift, pos, (Collisions) child, key) : 
                        dissocMayCollapse((Node) child, hash, key, lvl+5, node, shift, pos);
                    if (newnode == node) return parent;
                    return copyAndSet(parent, ppos-1, newnode, parent.count-1);
                }
                return lvl == 30 ? Collisions.dissoc(node, shift, pos, (Collisions) child, key) : dissocMayCollapse((Node) child, hash, key, lvl+5, parent, pshift, ppos);
            }
        }
        
        static Node merge(Node anc, Node a, Node b, IFn fix, Object notFound, int lvl) {
            // never, ever, ever return anc as anc is shared
            if (anc == a) return b; // also handles when anc and a are EMPTY
            if (anc == b || a == b) return a; // also handles when anc and b are EMPTY or when there's a closest common ancestor
            
            // fix is never called!?
            Object[] array = new Object[64];
            long bitmap = 0;
            int i = array.length;
            int count = 0;
            long bmab =  ((a.bitmap | a.bitmap >>> 1 | b.bitmap | b.bitmap >>> 1) & 6148914691236517205L);
            for(int shift = Long.numberOfTrailingZeros(bmab); shift < 64; shift += 2 + Long.numberOfTrailingZeros(bmab >>> (shift+2))) {
                int ai, bi;
                final Object ka, kb, va, vb, vanc, vr;
                
                switch((int) (((a.bitmap >>> shift) & 3L) << 2 | ((b.bitmap >>> shift) & 3L))) {
                case 3: // no a, kv b
                    bi = Long.bitCount(b.bitmap >>> shift);
                    kb = b.array[bi-2];
                    vb = b.array[bi-1];
                    vanc = lookup(anc, kb, notFound, lvl);
                    vr = vanc == notFound ? vb : fix.invoke(vanc, notFound, vb, notFound);
                    if (vr != notFound) {
                        bitmap |= 3L << shift;
                        count++;
                        array[--i] = vr;
                        array[--i] = kb;
                    }
                    continue;
                case 12: // kv a, no b
                    ai = Long.bitCount(a.bitmap >>> shift);
                    ka = a.array[ai-2];
                    va = a.array[ai-1];
                    vanc = lookup(anc, ka, notFound, lvl);
                    vr = vanc == notFound ? va : fix.invoke(vanc, va, notFound, notFound);
                    if (vr != notFound) {
                        bitmap |= 3L << shift;
                        count++;
                        array[--i] = vr;
                        array[--i] = ka;
                    }
                    continue;
                case 15: // kv a, kv b
                    ai = Long.bitCount(a.bitmap >>> shift);
                    ka = a.array[ai-2];
                    va = a.array[ai-1];
                    bi = Long.bitCount(b.bitmap >>> shift);
                    kb = b.array[bi-2];
                    vb = b.array[bi-1];
                    if (!Util.equiv(ka, kb)) break;
                    if (va == vb) {
                        vr = vb;                      
                    } else {
                        vanc = lookup(anc, ka, notFound, lvl);
                        vr = vanc == va ? vb : vanc == vb ? va : fix.invoke(vanc, va, vb, notFound);  
                    }
                    if (vr != notFound) {
                        bitmap |= 3L << shift;
                        count++;
                        array[--i] = vr;
                        array[--i] = kb;
                    }
                    continue;
                }
                if (lvl < 30) {
                    Node nanc = node(anc.array, anc.bitmap, shift, lvl);
                    Node na = node(a.array, a.bitmap, shift, lvl);
                    Node nb = node(b.array, b.bitmap, shift, lvl);
                    Node r = merge(nanc, na, nb, fix, notFound, lvl + 5);
                    if (r == null) continue;
                    count += r.count;
                    if (r.count == 1) {
                        bitmap |= 3L << shift;
                        array[--i] = r.array[1];
                        array[--i] = r.array[0];
                        continue;
                    }
                    array[--i] = r;
                    bitmap |= (r == na) ? a.bitmap & (3L << shift)
                            : (r == nb) ? b.bitmap & (3L << shift)
                                    : 2L << shift;
                } else {
                    Collisions nanc = collisions(anc.array, anc.bitmap, shift);
                    Collisions na = collisions(a.array, a.bitmap, shift);
                    Collisions nb = collisions(b.array, b.bitmap, shift);
                    Collisions r = Collisions.merge(nanc, na, nb, fix, notFound);
                    if (r == null) continue;
                    count += r.count;
                    if (r.count == 1) {
                        bitmap |= 3L << shift;
                        array[--i] = r.array[1];
                        array[--i] = r.array[0];
                        continue;
                    }
                    array[--i] = r;
                    bitmap |= (r == na) ? a.bitmap & (3L << shift)
                            : (r == nb) ? b.bitmap & (3L << shift)
                                    : 2L << shift;
                }
            }
            if (count == 0) return null;
            Object[] ra = new Object[Long.bitCount(bitmap)];
            System.arraycopy(array, i, ra, 0, array.length - i);
            return new Node(bitmap, ra, count);
        }

        private static Node node(Object[] array, long bitmap, int shift, int lvl) {
            switch((int) ((bitmap >>> shift) & 3L)) {
            case 0: return EMPTY;
            case 1: case 2: return (Node) array[Long.bitCount(bitmap >>> shift)-1];
            default:
               int pos = Long.bitCount(bitmap >>> shift);
                Object k = array[pos-2];
                Object v = array[pos-1];
                return new Node(3L << ((Util.hasheq(k) >>> (lvl + 5)) & 31), new Object[] {k, v}, 1);
            }
        }

        private static Collisions collisions(Object[] array, long bitmap, int shift) {
            switch((int) ((bitmap >>> shift) & 3L)) {
            case 0: return Collisions.EMPTY;
            case 1: case 2: return (Collisions) array[Long.bitCount(bitmap >>> shift)-1];
            default:
               int pos = Long.bitCount(bitmap >>> shift);
                Object k = array[pos-2];
                Object v = array[pos-1];
                return new Collisions(Util.hasheq(k), new Object[] {k, v}, 1);
            }
        }

        static Object lookup(Node node, Object key, Object notFound, int lvl) {
            int h = Util.hasheq(key);
            loop: for (;;) {
                int shift = ((h >>> lvl) & 31)*2;
                long lead = node.bitmap >>> shift;
                int pos = Long.bitCount(lead);
                int bits = (int) (lead & 3L);
                switch (bits) {
                case 0:
                    return notFound;
                case 3:
                    return Util.equiv(key, node.array[pos-2]) ? node.array[pos-1] : notFound;
                default:
                    lvl+=5;
                    if (lvl >= 32) return Collisions.lookup(node.array[pos-1], key, notFound);
                    node = (Node) node.array[pos-1];
                    continue loop;
                }
            }
        }
    }
    
    static private final class Collisions {
        
        static final Collisions EMPTY = new Collisions(0, new Object[0], 0);
        
        static class Seq extends ASeq {
            final Object[] array;
            final int idx;
            final ISeq nexts;

            static ISeq create(Object collisions_, ISeq nexts) {
                Collisions child = (Collisions) collisions_;
                return Collisions.Seq.create(child.array, (child.count-1)*2, nexts);
            }
            
            private static ISeq create(Object[] array, int idx, ISeq nexts) {
                if (idx < 0) return nexts;
                return new Seq(null, array, idx, nexts);
            }
            
            private Seq(IPersistentMap meta, Object[] array, int idx, ISeq nexts) {
                super(meta);
                this.array = array;
                this.idx = idx;
                this.nexts = nexts;
            }

            public Object first() {
                return new MapEntry(array[idx], array[idx+1]);
            }

            public ISeq next() {
                return create(array, idx-2, nexts);
            }

            public Obj withMeta(IPersistentMap meta) {
                return new Seq(meta, array, idx, nexts);
            }
            
        }
        
        int count;
        int hash;
        Object array[];
        
        Collisions(int hash, Object[] array, int count) {
            this.count = count;
            this.hash = hash;
            this.array = array;
        }

        static int indexOf(Collisions collisions, Object key) {
            int i = 2*(collisions.count - 1);
            for(; i >= 0; i-=2) 
                if (Util.equiv(collisions.array[i], key)) return i;
            return i; // -2
        }

        static Collisions extend(Collisions collisions, Object key, Object val) {
            int idx = Collisions.indexOf(collisions, key);
            if (idx >= 0) {
                if (collisions.array[idx+1] == val)
                    return collisions;
                return new Collisions(collisions.hash, aset(collisions.array, idx+1, val), collisions.count);            
            }
            int len = 2*collisions.count;
            int count = collisions.count + 1;
            Object[] array = new Object[2*count];
            System.arraycopy(collisions.array, 0, array, 0, len);
            array[len] = key;
            array[len+1] = val;
            return new Collisions(collisions.hash, array, count);
        }

        static Node dissoc(Node node, int shift, int pos, Collisions collisions, Object key) {
            int idx = Collisions.indexOf(collisions, key);
            if (idx < 0) return node;
            int count = collisions.count - 1;
            if (count == 1) {
                idx ^= 2; // the remaining index
                return Node.copyAndDemote(node, shift, pos, collisions.array[idx], collisions.array[idx+1]);
            }
            int len = 2*count;
            Object[] array = new Object[len];
            System.arraycopy(collisions.array, 0, array, 0, len);
            array[idx] = collisions.array[len];
            array[idx+1] = collisions.array[len+1];
            return Node.copyAndSet(node, pos, new Collisions(collisions.hash, array, count), node.count-1);
        }

        static Object lookup(Object collisions_, Object key, Object notFound) {
            Collisions collisions = (Collisions) collisions_;
            int i = indexOf(collisions, key);
            if (i < 0) return notFound;
            return collisions.array[i+1];
        }

        static IMapEntry lookup(Object collisions_, Object key) {
            Collisions collisions = (Collisions) collisions_;
            int i = indexOf(collisions, key);
            if (i < 0) return null;
            return new MapEntry(collisions.array[i], collisions.array[i+1]);
        }
        
        static Collisions merge(Collisions anc, Collisions a, Collisions b, IFn fix, Object notFound) {
            int len = (a.count + b.count)*2;
            Object[] array = new Object[len];
            int lim = 2*a.count;
            System.arraycopy(a.array, 0, array, 0, lim);
            System.arraycopy(b.array, 0, array, lim, len - lim);
            int i = 0;
            while(i < lim) {
                Object ka = array[i];
                Object va = array[i+1];
                int j = lim;
                while((j < len) && !Util.equiv(ka, array[j])) j+=2;
                Object vb = notFound;
                if (j < len) {
                    vb = array[j+1];
                    array[j+1] = array[--len];
                    array[j] = array[--len];
                }
                
                if (va == vb) { i+=2; continue; }
                
                Object vanc = lookup(anc, ka, notFound);
                if (vanc == vb) { i+=2; continue; }
                    
                Object vr = fix.invoke(vanc, va, vb);
                if (vr != notFound) {
                    array[i+1] = vr;
                    i+=2;
                } else { // deletion
                    array[i+1] = array[--lim];
                    array[lim] = array[--len];
                    array[i] = array[--lim];
                    array[lim] = array[--len];
                }
            }
            // a is exhausted
            while(i < len) {
                Object kb = array[i];
                Object vb = array[i+1];
                Object vanc = lookup(anc, kb, notFound);
                if (vanc == notFound) { i+=2; continue; }
                    
                Object vr = fix.invoke(vanc, notFound, vb);
                if (vr != notFound) {
                    array[i+1] = vr;
                    i+=2;
                } else { // deletion
                    array[i+1] = array[--len];
                    array[i] = array[--len];
                }
            }
            if (i == 0) return null;
            Object ra[] = new Object[i];
            System.arraycopy(array, 0, ra, 0, i);
            System.err.println(RT.seq(ra) + " " + i/2);
            int h = a.count > 0 ? a.hash : b.hash; // a and b should never be simultaneously empty
            return new Collisions(h, ra, i / 2);
        }
    }
    
    private Node root;
    
    public PersistentHashMapKVMono(Node root) {
        this.root = root;
    }
    
    public static final PersistentHashMapKVMono EMPTY = new PersistentHashMapKVMono(Node.EMPTY);

    static Object pushdown(int lvl, int hash, Object key, Object val, int h, Object k, Object v) {
        if (lvl < 32) {
            int idx = (hash >>> lvl) & 31;
            int i = (h >>> lvl) & 31;
            if (i == idx) return new Node(1L << (2 * idx), new Object[] { pushdown(lvl+5, hash, key, val, h, k, v) }, 2); // could be a loop
            return new Node(3L << (2 * idx) | 3L << (2 * i), idx < i ? new Object[] { k, v, key, val } : new Object[] { key, val, k, v }, 2);
        } else { // collisions only exist at the max depth
            return new Collisions(hash, new Object[] {k, v, key, val}, 2);
        }
    }

    static Object[] aset(Object[] array, int i, Object v) {
        Object[] a = array.clone();
        a[i] = v;
        return a;
    }

    public IPersistentMap assoc(Object key, Object val) {
        Node newroot = Node.assoc(root, Util.hasheq(key), key, val, 0);
        if (newroot == root) return this;
        return new PersistentHashMapKVMono(newroot);
    }    

    public IPersistentMap assocEx(Object key, Object val) {
        Node newroot = Node.assoc(root, Util.hasheq(key), key, val, 0);
        if (newroot.count == root.count)
            throw Util.runtimeException("Key already present");
        return new PersistentHashMapKVMono(newroot);
    }

    public IPersistentMap without(Object key) {
        Node newroot = Node.dissoc(root, Util.hasheq(key), key, 0);
        if (newroot == root) return this;
        return new PersistentHashMapKVMono(newroot);
    }

    public static PersistentHashMapKVMono merge(PersistentHashMapKVMono ancestor, PersistentHashMapKVMono a, PersistentHashMapKVMono b, IFn fix) {
        Node root = Node.merge(ancestor.root, a.root, b.root, fix, new Object(), 0);
        if (root == null) return EMPTY;
        return new PersistentHashMapKVMono(root);
    }
    
    public Iterator iterator() {
        // TODO gross
        return new SeqIterator(seq());
    }

    public boolean containsKey(Object key) {
        return valAt(key, root) != root;
    }

    public IMapEntry entryAt(Object key) {
        int hash = Util.hasheq(key);
        int h = hash;
        int hops = 7;
        Node node = root;
        loop: for (;;) {
            int shift = (h & 31)*2;
            long lead = node.bitmap >>> shift;
            int pos = Long.bitCount(lead);
            int bits = (int) (lead & 3L);
            switch (bits) {
            case 0:
                return null;
            case 3:
                Object k = node.array[pos-2];
                return Util.equiv(key, k) ? new MapEntry(k, node.array[pos-1]) : null;
            default:
                hops--;
                if (hops == 0) return Collisions.lookup(node.array[pos-1], key);
                h >>>= 5;
                node = (Node) node.array[pos-1];
                continue loop;
            }
        }
    }

    public int count() {
        return root.count;
    }

    public IPersistentCollection empty() {
        // TODO support meta
        return new PersistentHashMapKVMono(Node.EMPTY);
    }

    public ISeq seq() {
        return Node.Seq.create(root, null, 0);
    }

    public Object valAt(Object key) {
        return valAt(key, null);
    }

    public Object valAt(Object key, Object notFound) {
        return Node.lookup(root, key, notFound, 0);
    }
}

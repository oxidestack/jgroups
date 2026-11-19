import org.jgroups.*;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.DeltaView;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.PrintWriter;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Dumps byte vectors from the real jgroups-5.2.28.Final jar for jgroups-rs's wire codec tests.
 * Not hand-derived: every vector is produced by calling the actual upstream writeTo()/Bits methods.
 */
public class Main {

    static PrintWriter out;

    public static void main(String[] args) throws Exception {
        out = new PrintWriter(args[0], "UTF-8");

        // ---------------- Bits ----------------
        long[] longVals = {Long.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260,
                Short.MAX_VALUE, Integer.MIN_VALUE, 322649, Integer.MAX_VALUE,
                (long) Integer.MAX_VALUE + 100, Long.MAX_VALUE - 10, Long.MAX_VALUE};
        for (long v : longVals) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            Bits.writeLongCompressed(v, dos);
            line("bits.long." + v, Bits.size(v), bos.toByteArray());
        }

        int[] intVals = {Integer.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260,
                Short.MAX_VALUE, 322649, Integer.MAX_VALUE};
        for (int v : intVals) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            Bits.writeIntCompressed(v, dos);
            line("bits.int." + v, Bits.size(v), bos.toByteArray());
        }

        long[][] seqPairs = {{0, 0}, {1, 1}, {50, 150}, {127, 127}, {128, 128}, {254, 354},
                {255, 255}, {256, 256}, {Short.MAX_VALUE, Short.MAX_VALUE + 100},
                {100000, 100100}, {Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 100},
                {Long.MAX_VALUE / 10, Long.MAX_VALUE / 10 + 100}, {Long.MAX_VALUE - 1, Long.MAX_VALUE}};
        for (long[] p : seqPairs) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            Bits.writeLongSequence(p[0], p[1], dos);
            line("bits.longseq." + p[0] + "." + p[1], Bits.size(p[0], p[1]), bos.toByteArray());
        }

        writeStringVector("null", null);
        writeStringVector("empty", "");
        writeStringVector("ascii", "hello world");
        writeStringVector("cluster", "DemoChannel");

        // ---------------- Version ----------------
        out.println("version.value=" + Version.version);
        out.println("version.print=" + Version.print(Version.version));
        out.println("version.encode.5.2.28=" + Version.encode(5, 2, 28));
        out.println("version.encode.5.2.0=" + Version.encode(5, 2, 0));
        out.println("version.same.1=" + Version.isSame(Version.encode(5, 2, 28)));
        out.println("version.same.0=" + Version.isSame((short) 0));

        // ---------------- Addresses ----------------
        UUID u1 = new UUID(0x0102030405060708L, 0x1112131415161718L);
        writeVector("addr.uuid.body", u1);
        UUID u2 = new UUID(0L, 0L);
        writeVector("addr.uuid.body.zero", u2);
        UUID u3 = new UUID(-1L, -1L);
        writeVector("addr.uuid.body.neg1", u3);

        ExtendedUUID eu1 = new ExtendedUUID(0x0102030405060708L, 0x1112131415161718L);
        eu1.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        writeVector("addr.extuuid.body.k1", eu1);

        ExtendedUUID eu2 = new ExtendedUUID(42L, 43L);
        eu2.put("a", "1".getBytes(StandardCharsets.UTF_8));
        eu2.put("bb", "22".getBytes(StandardCharsets.UTF_8));
        writeVector("addr.extuuid.body.two", eu2);

        IpAddress ip4 = new IpAddress("192.168.1.5", 7800);
        writeVector("addr.ip4.body", ip4);
        IpAddress ip4b = new IpAddress("10.0.0.1", 65535);
        writeVector("addr.ip4.highport.body", ip4b);

        InetAddress in6 = Inet6Address.getByAddress(null,
                new byte[]{0x20, 0x01, 0x0d, (byte) 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 0);
        IpAddress ip6 = new IpAddress(new java.net.InetSocketAddress(in6, 7801));
        writeVector("addr.ip6.body", ip6);

        // Util.writeAddress dispatch (flags byte + body)
        writeAddrDispatch("addr.write.null", null);
        writeAddrDispatch("addr.write.uuid", u1);
        writeAddrDispatch("addr.write.extuuid", eu1);
        writeAddrDispatch("addr.write.ip4", ip4);
        writeAddrDispatch("addr.write.ip6", ip6);

        // ---------------- Headers ----------------
        writeHeader("hdr.tp.named", new TpHeader("DemoChannel"));
        writeHeader("hdr.tp.null", new TpHeader((byte[]) null));

        // headers of protocols jgroups-rs does not implement but must be able to read past (FD_SOCK, FD_SOCK2, VERIFY_SUSPECT)
        {
            UUID fu1 = new UUID(0x0102030405060708L, 0x1112131415161718L);
            UUID fu2 = new UUID(0x2122232425262728L, 0x3132333435363738L);
            writeHeader("hdr.fd_sock.suspect", new FD_SOCK.FdHeader(FD_SOCK.FdHeader.SUSPECT).mbrs(new java.util.LinkedHashSet<>(java.util.Arrays.asList(fu1, fu2))));
            writeHeader("hdr.fd_sock.i_have_sock", new FD_SOCK.FdHeader(FD_SOCK.FdHeader.I_HAVE_SOCK, fu1, new IpAddress(java.net.InetAddress.getByAddress(new byte[]{10, 0, 0, 1}), 7800)));
            writeHeader("hdr.fd_sock.who_has", new FD_SOCK.FdHeader(FD_SOCK.FdHeader.WHO_HAS_SOCK, fu2));
            writeHeader("hdr.fd_sock2.suspect", new FD_SOCK2.FdHeader(FD_SOCK2.FdHeader.SUSPECT).mbrs(java.util.Arrays.asList(fu1, fu2)));
            writeHeader("hdr.fd_sock2.connect_rsp", new FD_SOCK2.FdHeader(FD_SOCK2.FdHeader.CONNECT_RSP).cluster("cl").serverAddress(fu1));
            java.lang.reflect.Constructor<?> vc = Class.forName("org.jgroups.protocols.VERIFY_SUSPECT$VerifyHeader").getDeclaredConstructor(short.class, Address.class);
            vc.setAccessible(true);
            writeHeader("hdr.verify_suspect.i_am_not_dead", (Header) vc.newInstance((short) 2, fu1));
        }
        writeHeader("hdr.ping.req.bla", new PingHeader(PingHeader.GET_MBRS_REQ).clusterName("bla"));
        writeHeader("hdr.ping.rsp.noname", new PingHeader(PingHeader.GET_MBRS_RSP));
        writeHeader("hdr.ping.rsp.nullname", new PingHeader(PingHeader.GET_MBRS_RSP).clusterName(null));
        writeHeader("hdr.ping.rsp.cluster", new PingHeader(PingHeader.GET_MBRS_RSP).clusterName("cluster"));

        writeHeader("hdr.nakack.msg", NakAckHeader2.createMessageHeader(322649));
        writeHeader("hdr.nakack.xmitreq", NakAckHeader2.createXmitRequestHeader(u1));
        writeHeader("hdr.nakack.xmitrsp", NakAckHeader2.createXmitResponseHeader());
        writeHeader("hdr.nakack.highest", NakAckHeader2.createHighestSeqnoHeader(999));

        writeHeader("hdr.uni3.data", UnicastHeader3.createDataHeader(322649, (short) 127, false));
        writeHeader("hdr.uni3.data.maxconn", UnicastHeader3.createDataHeader(322649, Short.MAX_VALUE, false));
        writeHeader("hdr.uni3.data.first", UnicastHeader3.createDataHeader(322649, (short) (Short.MAX_VALUE - 10), true));
        writeHeader("hdr.uni3.sendfirst.0", UnicastHeader3.createSendFirstSeqnoHeader(0));
        writeHeader("hdr.uni3.sendfirst.100", UnicastHeader3.createSendFirstSeqnoHeader(100));
        writeHeader("hdr.uni3.ack", UnicastHeader3.createAckHeader(322649, (short) 2, 500600));
        writeHeader("hdr.uni3.xmitreq", UnicastHeader3.createXmitReqHeader());
        writeHeader("hdr.uni3.close", UnicastHeader3.createCloseHeader((short) 7));

        // ---------------- SeqnoList (NAKACK2/UNICAST3's XMIT_REQ payload) ----------------
        SeqnoList sl1 = new SeqnoList(20, 1000);
        sl1.add(1000).add(1005).add(1010);
        writeSeqnoListVector("seqnolist.small", sl1);

        SeqnoList slEmpty = new SeqnoList(50, 5000);
        writeSeqnoListVector("seqnolist.empty", slEmpty);

        SeqnoList slWordBoundary = new SeqnoList(200, 1000);
        slWordBoundary.add(1000).add(1063).add(1064).add(1199);
        writeSeqnoListVector("seqnolist.wordboundary", slWordBoundary);

        SeqnoList slRange = new SeqnoList(20, 100);
        slRange.add(102, 106);
        writeSeqnoListVector("seqnolist.range", slRange);

        ViewId vid = new ViewId(u1, 1);
        writeHeader("hdr.stable.gossip", new STABLE.StableHeader(STABLE.StableHeader.STABLE_GOSSIP, vid));
        writeHeader("hdr.stable.stability.null", new STABLE.StableHeader(STABLE.StableHeader.STABILITY, null));

        writeHeader("hdr.merge.info", MERGE3.MergeHeader.createInfo(vid, "A", ip4));
        writeHeader("hdr.merge.viewreq", MERGE3.MergeHeader.createViewRequest());
        writeHeader("hdr.merge.viewrsp", MERGE3.MergeHeader.createViewResponse());

        writeHeader("hdr.heartbeat", new FailureDetection.HeartbeatHeader());
        writeHeader("hdr.verify.areyoudead", makeVerifyHeader((short) 1, u1));
        writeHeader("hdr.verify.iamnotdead.null", makeVerifyHeader((short) 2, null));

        GMS.GmsHeader gh1 = new GMS.GmsHeader(GMS.GmsHeader.JOIN_REQ, u1, true);
        writeHeader("hdr.gms.joinreq", gh1);
        GMS.GmsHeader gh2 = new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, u1, false);
        writeHeader("hdr.gms.leavereq", gh2);
        GMS.GmsHeader gh3 = new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ);
        gh3.setMergeId(MergeId.create(u1));
        writeHeader("hdr.gms.mergereq", gh3);
        GMS.GmsHeader gh4 = new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        gh4.setMergeId(MergeId.create(u2));
        gh4.setMergeRejected(true);
        writeHeader("hdr.gms.mergersp.rejected", gh4);
        GMS.GmsHeader gh5 = new GMS.GmsHeader(GMS.GmsHeader.VIEW);
        writeHeader("hdr.gms.view.nombr", gh5);

        writeHeader("hdr.fc.replenish", new FcHeader(FcHeader.REPLENISH));
        writeHeader("hdr.fc.creditreq", new FcHeader(FcHeader.CREDIT_REQUEST));

        FragHeader fh = new FragHeader(12345, 2, 5);
        fh.needsDeserialization(true);
        fh.setOriginalType((short) 3);
        writeHeader("hdr.frag", fh);

        writeHeader("hdr.reqcorr.req", new RequestCorrelator.Header(RequestCorrelator.Header.REQ, 42, (short) 7));
        writeHeader("hdr.reqcorr.rsp", new RequestCorrelator.Header(RequestCorrelator.Header.RSP, 0, (short) 7));

        // ---------------- Messages (single frame, Util.writeMessage) ----------------
        BytesMessage bm = new BytesMessage(ip4, "payload-bytes".getBytes(StandardCharsets.UTF_8));
        bm.setSrc(u1);
        bm.putHeader((short) 60, new TpHeader("cl"));
        writeMessageVector("msg.bytes.full", bm, false);

        BytesMessage bmEmpty = new BytesMessage(null);
        writeMessageVector("msg.bytes.nulldest.nopayload", bmEmpty, true);

        EmptyMessage em = new EmptyMessage(u1);
        em.putHeader((short) 62, new FailureDetection.HeartbeatHeader());
        writeMessageVector("msg.empty", em, false);

        LongMessage lm = new LongMessage(ip4, 123456789L);
        writeMessageVector("msg.long", lm, false);

        // ---------------- MessageBatch header framing (Util.writeMessageListHeader) ----------------
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            byte[] clusterName = "democluster".getBytes(StandardCharsets.UTF_8);
            Util.writeMessageListHeader(ip4, u1, clusterName, 2, dos, false);
            line("batch.header", -1, bos.toByteArray());
        }

        // ---------------- TCP handshake (blocks.cs.TcpConnection.sendLocalAddress) ----------------
        // Cookie + version + addr_size + the local IpAddress body, exactly what TcpConnection writes when
        // connecting with use_peer_connections=true (which TCP.start() always sets).
        {
            IpAddress local = new IpAddress("127.0.0.1", 7800);
            int addrSize = local.serializedSize();
            ByteArrayDataOutputStream os =
                    new ByteArrayDataOutputStream(addrSize + Short.BYTES * 2 + Connection.cookie.length);
            os.write(Connection.cookie, 0, Connection.cookie.length);
            os.writeShort(Version.version);
            os.writeShort(addrSize);
            local.writeTo(os);
            line("tcp.handshake.ip4", -1, java.util.Arrays.copyOf(os.buffer(), os.position()));
        }
        {
            IpAddress local = new IpAddress("10.20.30.40", 65000);
            int addrSize = local.serializedSize();
            ByteArrayDataOutputStream os =
                    new ByteArrayDataOutputStream(addrSize + Short.BYTES * 2 + Connection.cookie.length);
            os.write(Connection.cookie, 0, Connection.cookie.length);
            os.writeShort(Version.version);
            os.writeShort(addrSize);
            local.writeTo(os);
            line("tcp.handshake.ip4.highport", -1, java.util.Arrays.copyOf(os.buffer(), os.position()));
        }

        // ---------------- PingData (protocols/PingData.java, TCPPING discovery payload) ----------------
        writePingData("pingdata.full", new PingData(u1, true, "nodeA", ip4).mbrs(java.util.Arrays.asList(u1, eu1)));
        writePingData("pingdata.minimal", new PingData(u1, false));
        writePingData("pingdata.coord", new PingData(u1, true).coord(true));
        writePingData("pingdata.no_physical_addr", new PingData(u1, true, "nodeB", null));
        {
            // Discovery.marshal(PingData) = Util.streamableToBuffer: presence byte + writeTo
            PingData pd = new PingData(u1, true, "nodeA", ip4).coord(true);
            org.jgroups.util.ByteArray ba = Util.streamableToBuffer(pd);
            line("pingdata.streamable.coord", -1, java.util.Arrays.copyOfRange(ba.getArray(), ba.getOffset(), ba.getOffset() + ba.getLength()));
        }
        writePingData("pingdata.no_mbrs", new PingData(ip4, true, "nodeC", ip4b));

        // ---------------- View / Digest / JoinRsp (GMS join) ----------------
        View viewSingleton = View.create(u1, 0, u1);
        writeViewVector("view.singleton", viewSingleton);

        View viewTwo = View.create(u1, 1, u1, u2);
        writeViewVector("view.two", viewTwo);

        View viewEmpty = new View(new ViewId(u1, 3), new Address[]{});
        writeViewVector("view.empty", viewEmpty);

        Digest digestSingletonZero = new Digest(u1, 0, 0);
        writeDigestVector("digest.singleton.zero.withaddrs", digestSingletonZero, true);
        writeDigestVector("digest.singleton.zero.noaddrs", digestSingletonZero, false);

        Digest digestTwo = new Digest(new Address[]{u1, u2}, new long[]{5, 10, 0, 0});
        writeMessageVector("msg.obj.digest", new ObjectMessage(null, digestTwo), true);
        writeMessageVector("msg.obj.mutabledigest", new ObjectMessage(null, new MutableDigest(digestTwo)), true);
        writeDigestVector("digest.two.withaddrs", digestTwo, true);
        writeDigestVector("digest.two.noaddrs", digestTwo, false);

        JoinRsp joinRspFull = new JoinRsp(viewTwo, digestTwo);
        writeJoinRspVector("joinrsp.full", joinRspFull);

        JoinRsp joinRspFail = new JoinRsp("auth failed");
        writeJoinRspVector("joinrsp.fail", joinRspFail);
        // GMS.marshal(JoinRsp) = Util.streamableToBuffer: presence byte + writeTo (what a JOIN_RSP message carries)
        for (String[] e : new String[][]{{"full", "0"}, {"fail", "1"}}) {
            org.jgroups.util.ByteArray ba = Util.streamableToBuffer(e[1].equals("0") ? joinRspFull : joinRspFail);
            line("joinrsp.streamable." + e[0], -1, java.util.Arrays.copyOfRange(ba.getArray(), ba.getOffset(), ba.getOffset() + ba.getLength()));
        }

        // GMS.castViewChangeAndSendJoinRsps' wire format for a VIEW message's payload:
        // short flags (GMS.determineFlags), then view.writeTo(), then digest.writeTo(write_addrs) --
        // GMS.marshal(View, Digest) is package-private, so this reproduces it from public APIs only
        // (GMS.java:1138-1158).
        {
            // MergeView: subgroups made of members of the merged view, and one with a
            // foreign member (written as an address, index -1, JGRP-1707)
            UUID u3m = new UUID(0x4142434445464748L, 0x5152535455565758L);
            MergeView mv = new MergeView(viewTwo.getViewId(), java.util.Arrays.asList(u1, u2),
                    java.util.Arrays.asList(View.create(u1, 0, u1), View.create(u2, 5, u2)));
            writeViewVector("mergeview.two", mv);
            MergeView mvf = new MergeView(viewTwo.getViewId(), java.util.Arrays.asList(u1, u2),
                    java.util.Arrays.asList(View.create(u1, 3, u1, u3m), View.create(u3m, 4, u3m)));
            writeViewVector("mergeview.foreign", mvf);
            writeViewDigestVector("viewdigest.merge", mv, digestTwo);
        }
        {
            // DeltaView: the VIEW payload a coordinator sends with
            // use_delta_views. A joiner, a leaver, and one carrying a digest.
            UUID u3d = new UUID(0x4142434445464748L, 0x5152535455565758L);
            DeltaView dvJoin = new DeltaView(new ViewId(u1, 2), new ViewId(u1, 1), new Address[]{}, new Address[]{u3d});
            writeViewVector("deltaview.join", dvJoin);
            DeltaView dvLeave = new DeltaView(new ViewId(u1, 3), new ViewId(u1, 2), new Address[]{u2}, new Address[]{});
            writeViewVector("deltaview.leave", dvLeave);
            writeViewDigestVector("viewdigest.delta", dvJoin, null);
            writeViewDigestVector("viewdigest.delta.digest", dvLeave, digestTwo);
        }
        {
            // What a coordinator with use_delta_views multicasts after its first view,
            // through the real GMS.createDeltaView and GMS.marshal(View, Digest) (protected static, reached by
            // reflection), with the digest castViewChange is handed: a joiner, then that member leaving.
            UUID u3d = new UUID(0x4142434445464748L, 0x5152535455565758L);
            java.lang.reflect.Method createDelta = GMS.class.getDeclaredMethod("createDeltaView", View.class, View.class);
            createDelta.setAccessible(true);
            java.lang.reflect.Method marshal = GMS.class.getDeclaredMethod("marshal", View.class, Digest.class);
            marshal.setAccessible(true);
            View two = View.create(u1, 1, u1, u2);
            View three = View.create(u1, 2, u1, u2, u3d);
            View back = View.create(u1, 3, u1, u3d);
            Digest joinDigest = new Digest(new Address[]{u1, u2, u3d}, new long[]{5, 10, 0, 0, 0, 0});
            Digest leaveDigest = new Digest(new Address[]{u1, u3d}, new long[]{0, 0, 0, 0});
            for (Object[] c : new Object[][]{{"join", two, three, joinDigest}, {"leave", three, back, leaveDigest}}) {
                View delta = (View) createDelta.invoke(null, c[1], c[2]);
                org.jgroups.util.ByteArray ba = (org.jgroups.util.ByteArray) marshal.invoke(null, delta, c[3]);
                line("gms.castview.delta." + c[0], -1, java.util.Arrays.copyOfRange(ba.getArray(), ba.getOffset(), ba.getOffset() + ba.getLength()));
            }
            // the coordinator's first view (first_view_sent false) is the full view
            org.jgroups.util.ByteArray first = (org.jgroups.util.ByteArray) marshal.invoke(null, three, joinDigest);
            line("gms.castview.full.join", -1, java.util.Arrays.copyOfRange(first.getArray(), first.getOffset(), first.getOffset() + first.getLength()));
        }
        {
            // The payload of MERGE3's VIEW_RSP, MERGE3.marshal(view) (Util.streamableToBuffer:
            // a presence byte, then the view), for a view and for none (a member that has no view yet).
            View two = View.create(u1, 1, u1, u2);
            org.jgroups.util.ByteArray rsp = MERGE3.marshal(two);
            line("merge3.viewrsp.view", -1, java.util.Arrays.copyOfRange(rsp.getArray(), rsp.getOffset(), rsp.getOffset() + rsp.getLength()));
            org.jgroups.util.ByteArray none = MERGE3.marshal(null);
            line("merge3.viewrsp.null", -1, java.util.Arrays.copyOfRange(none.getArray(), none.getOffset(), none.getOffset() + none.getLength()));
        }
        writeViewDigestVector("viewdigest.same_members", viewTwo, digestTwo); // members match -> write_addrs=false
        Digest digestSubset = new Digest(u1, 0, 0); // members differ from viewTwo's -> write_addrs=true
        writeViewDigestVector("viewdigest.different_members", viewTwo, digestSubset);
        writeViewDigestVector("viewdigest.view_only", viewSingleton, null);

        // ---------------- Protocol ids / magic ids (ClassConfigurator, from jg-protocol-ids.xml / jg-magic-map.xml) ----------------
        String[] protos = {"org.jgroups.protocols.FRAG2", "org.jgroups.protocols.TCPPING", "org.jgroups.protocols.pbcast.GMS",
                "org.jgroups.protocols.pbcast.STABLE", "org.jgroups.protocols.TCP", "org.jgroups.protocols.SHARED_LOOPBACK",
                "org.jgroups.protocols.MFC", "org.jgroups.protocols.UFC", "org.jgroups.protocols.MERGE3",
                "org.jgroups.protocols.pbcast.NAKACK2", "org.jgroups.protocols.UNICAST3", "org.jgroups.protocols.SHARED_LOOPBACK_PING",
                "org.jgroups.protocols.TP", "org.jgroups.protocols.dns.DNS_PING", "org.jgroups.protocols.FD_ALL3",
                "org.jgroups.protocols.VERIFY_SUSPECT2", "org.jgroups.protocols.FD_SOCK", "org.jgroups.protocols.FD_SOCK2",
                "org.jgroups.protocols.TCP_NIO2", "org.jgroups.protocols.FD_ALL"};
        for (String c : protos) {
            Class<?> cl = Class.forName(c);
            out.println("ids.proto." + cl.getSimpleName() + "=" + org.jgroups.conf.ClassConfigurator.getProtocolId((Class<? extends org.jgroups.stack.Protocol>) cl));
        }
        String[] magics = {"org.jgroups.protocols.FragHeader", "org.jgroups.protocols.PingHeader", "org.jgroups.protocols.pbcast.GMS$GmsHeader",
                "org.jgroups.protocols.pbcast.STABLE$StableHeader", "org.jgroups.protocols.FcHeader", "org.jgroups.protocols.TpHeader",
                "org.jgroups.protocols.FailureDetection$HeartbeatHeader", "org.jgroups.blocks.RequestCorrelator$Header",
                "org.jgroups.protocols.MERGE3$MergeHeader", "org.jgroups.protocols.pbcast.NakAckHeader2", "org.jgroups.protocols.UnicastHeader3",
                "org.jgroups.protocols.VERIFY_SUSPECT2$VerifyHeader", "org.jgroups.View", "org.jgroups.MergeView", "org.jgroups.ViewId",
                "org.jgroups.util.UUID", "org.jgroups.util.ExtendedUUID", "org.jgroups.stack.IpAddress", "org.jgroups.BytesMessage",
                "org.jgroups.util.SeqnoList", "org.jgroups.util.Digest"};
        for (String c : magics) {
            Class<?> cl;
            try { cl = Class.forName(c); } catch (ClassNotFoundException e) { continue; }
            out.println("ids.magic." + cl.getName().substring(cl.getName().lastIndexOf('.') + 1).replace('$', '.') + "=" + org.jgroups.conf.ClassConfigurator.getMagicNumber(cl));
        }

        out.flush();
        out.close();
        System.out.println("wrote vectors to " + args[0]);
    }

    static VERIFY_SUSPECT2.VerifyHeader makeVerifyHeader(short type, Address from) throws Exception {
        java.lang.reflect.Constructor<VERIFY_SUSPECT2.VerifyHeader> ctor =
                VERIFY_SUSPECT2.VerifyHeader.class.getDeclaredConstructor(short.class, Address.class);
        ctor.setAccessible(true);
        return ctor.newInstance(type, from);
    }

    static void writeStringVector(String label, String s) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Bits.writeString(s, dos);
        line("bits.string." + label, Bits.size(s), bos.toByteArray());
    }

    static void writeVector(String name, Address addr) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        addr.writeTo(dos);
        line(name, addr.serializedSize(), bos.toByteArray());
    }

    static void writeAddrDispatch(String name, Address addr) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Util.writeAddress(addr, dos);
        line(name, Util.size(addr), bos.toByteArray());
    }

    static void writeHeader(String name, Header hdr) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        hdr.writeTo(dos);
        line(name, hdr.serializedSize(), bos.toByteArray());
    }

    static void writeMessageVector(String name, Message msg, boolean multicast) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Util.writeMessage(msg, dos, multicast);
        line(name, -1, bos.toByteArray());
    }

    static void writePingData(String name, PingData pd) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        pd.writeTo(dos);
        line(name, pd.serializedSize(), bos.toByteArray());
    }

    static void writeViewVector(String name, View v) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        v.writeTo(dos);
        line(name, v.serializedSize(), bos.toByteArray());
    }

    static void writeDigestVector(String name, Digest d, boolean writeAddrs) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        d.writeTo(dos, writeAddrs);
        line(name, d.serializedSize(writeAddrs), bos.toByteArray());
    }

    static void writeSeqnoListVector(String name, SeqnoList sl) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        sl.writeTo(dos);
        line(name, sl.serializedSize(), bos.toByteArray());
    }

    static void writeJoinRspVector(String name, JoinRsp jr) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        jr.writeTo(dos);
        line(name, jr.serializedSize(), bos.toByteArray());
    }

    /** Reproduces GMS.marshal(View, Digest)/determineFlags/writeAddresses from public APIs only
     *  (that method is package-private on GMS) -- see the call site's comment for the source lines. */
    static void writeViewDigestVector(String name, View v, Digest d) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        boolean writeAddrs = d == null || v == null || !java.util.Arrays.equals(v.getMembersRaw(), d.getMembersRaw());
        short flags = 0;
        if (v != null) flags |= 1; // VIEW_PRESENT
        if (v instanceof MergeView) flags |= 4; // MERGE_VIEW
        else if (v instanceof DeltaView) flags |= 8; // DELTA_VIEW
        if (d != null) flags |= 2; // DIGEST_PRESENT
        if (writeAddrs) flags |= 16; // READ_ADDRS
        dos.writeShort(flags);
        if (v != null) v.writeTo(dos);
        if (d != null) d.writeTo(dos, writeAddrs);
        line(name, -1, bos.toByteArray());
    }

    static void line(String name, long size, byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append('=').append(size).append(':');
        for (byte b : bytes) sb.append(String.format(Locale.ROOT, "%02x", b));
        out.println(sb);
    }
}

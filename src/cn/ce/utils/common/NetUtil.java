package cn.ce.utils.common;

import java.net.InetAddress;

//import jpcap.JpcapCaptor;
//import jpcap.JpcapSender;
//import jpcap.NetworkInterface;
//import jpcap.packet.ARPPacket;
//import jpcap.packet.EthernetPacket;
//import jpcap.packet.Packet;

public class NetUtil {
	//
	// // ARP reply包生成类，用于根据目的地址和源地址生成reply包
	// // 前面是发送的目标地址
	// // 后面是被修改arp项
	// private static ARPPacket genARPPacket(IpMacMap sendTo, IpMacMap fake)
	// throws Exception {
	// ARPPacket arpTarget = new ARPPacket();
	// arpTarget.hardtype = ARPPacket.HARDTYPE_ETHER; // 选择以太网类型(Ethernet)
	// arpTarget.prototype = ARPPacket.PROTOTYPE_IP; // 选择IP网络协议类型
	// arpTarget.operation = ARPPacket.ARP_REPLY; // 选择REPLY类型
	// arpTarget.hlen = 6; // MAC地址长度固定6个字节
	// arpTarget.plen = 4; // IP地址长度固定4个字节
	// arpTarget.target_hardaddr = sendTo.getMac();
	// arpTarget.target_protoaddr = InetAddress.getByName(sendTo.getIp())
	// .getAddress();
	// //
	// arpTarget.sender_hardaddr = fake.getMac();
	// arpTarget.sender_protoaddr = InetAddress.getByName(fake.getIp())
	// .getAddress();
	// return arpTarget;
	// }
	//
	// // 根据目的地MAC和源MAC构建以太网头信息，用于传输数据
	// private static EthernetPacket genEthernetPacket(byte[] sendTo,
	// byte[] sendSrc) throws Exception {
	// EthernetPacket ethToTarget = new EthernetPacket(); // 创建一个以太网头
	// ethToTarget.frametype = EthernetPacket.ETHERTYPE_ARP; // 选择以太包类型
	// ethToTarget.dst_mac = sendTo;
	// ethToTarget.src_mac = sendSrc;
	// return ethToTarget;
	// }
	//
	// public static void sendPackage(String devName, String fakeIp,
	// byte[] fakeMac, String sendToIp, byte[] sendToMac,
	// String sendSrcIp, byte[] sendSrcMac) throws Exception {
	// IpMacMap fackIpMacMap = new IpMacMap(fakeIp, fakeMac);
	// IpMacMap sendToIpMacMap = new IpMacMap(sendToIp, sendToMac);
	// NetworkInterface[] devices = JpcapCaptor.getDeviceList();
	// NetworkInterface device = null;
	// for (NetworkInterface dev : devices) {
	// if (devName.equals(dev.name)) {
	// device = dev;
	// break;
	// }
	// }
	// System.out.println("---------device:" + device.name);
	// JpcapCaptor jpcap = JpcapCaptor.openDevice(device, 2000, false, 10000);
	// // 打开与设备的连接
	// jpcap.setFilter("ip", true); // 只监听ip数据包
	// JpcapSender sender = jpcap.getJpcapSenderInstance();
	// // reply包的源IP和MAC地址，此IP-MAC对将会被映射到ARP表
	// // 创建修改目标机器ARP的包
	// Packet replyPacket = NetUtil.genARPPacket(sendToIpMacMap, fackIpMacMap);
	// // 创建以太网头信息，并打包进reply包
	// replyPacket.datalink = NetUtil.genEthernetPacket(sendToMac, sendSrcMac);
	// sender.sendPacket(replyPacket);
	// jpcap.close();
	// }
	//
	// public static void testSendPackage(String devName) throws Exception {
	// // 172.23.150.106 c4:2c:03:01:e5:2b
	// final String fakeIp = "172.23.150.106";
	// final byte[] fakeMac = { (byte) 0xc4, (byte) 0x2c, (byte) 0x03,
	// (byte) 0x01, (byte) 0xe5, (byte) 0x2b };
	// //
	// final String sendToIp = "172.23.151.254";
	// // 传输使用
	// // 8:19:a6:2a:c7:74 gate
	// // c4:2c:03:01:e5:2c mymac
	// final byte[] sendSrcMac = { (byte) 0xc4, (byte) 0x2c, (byte) 0x03,
	// (byte) 0x01, (byte) 0xe5, (byte) 0x2c };
	// final byte[] sendToMac = { (byte) 0x08, (byte) 0x19, (byte) 0xa6,
	// (byte) 0x2a, (byte) 0xc7, (byte) 0x74 };
	//
	// for (int i = 0; i < 1000000; i++) {
	// NetUtil.sendPackage(devName, fakeIp, fakeMac, sendToIp, sendToMac,
	// fakeIp, sendSrcMac);
	// System.out.println("---------Send one package num:" + i + "------");
	// Thread.sleep(1000);
	// }
	//
	// }
	//
	// public static void main(String args[]) {
	// try {
	// NetUtil.testSendPackage("en0");
	// System.out.println("---------Over------");
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// }
	//
	// // IP-MAC实体，只用于保存一对IP-MAC地址
	// class IpMacMap {
	// private String ip;
	// private byte[] mac;
	//
	// public IpMacMap() {
	// }
	//
	// public IpMacMap(String ip, byte[] mac) {
	// this.ip = ip;
	// this.mac = mac;
	// }
	//
	// public String getIp() {
	// return ip;
	// }
	//
	// public void setIp(String ip) {
	// this.ip = ip;
	// }
	//
	// public byte[] getMac() {
	// return mac;
	// }
	//
	// public void setMac(byte[] mac) {
	// this.mac = mac;
	// }

}

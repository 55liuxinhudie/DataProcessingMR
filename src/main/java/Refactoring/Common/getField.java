package Refactoring.Common;

import java.util.Arrays;
import java.util.List;

public class getField {

	public static String  getWebsite_cd(String webSite, String ps, String pu) {
		Integer webSite_cd = 99;
		if (webSite.equals("10000001")){
			//联想商城
			String [] lenovoString = {"www.lenovo.com.cn","cart.lenovo.com.cn","order.lenovo.com.cn","i.lenovo.com.cn","z.lenovo.com.cn","s.lenovo.com.cn","coupon.lenovo.com.cn","m.lenovo.com.cn","m.cart.lenovo.com.cn","3g.lenovo.com.cn","app_host_name","m.order.lenovo.com.cn","m.coupon.lenovo.com.cn"};
			List<String> lenovoList = Arrays.asList(lenovoString);
			if (lenovoList.contains(ps) || 
				(ps.equals("pay.i.lenovo.com") && (pu.contains("plat=4")||pu.contains("b2cPc_url")||pu.contains("plat=1")||pu.contains("b2cWap_url")||pu.contains("plat=3")))
				|| (ps.equals("c.lenovo.com.cn") && pu.contains("lenovo"))
				|| (pu.toLowerCase().contains("shopid=1"))
				){
				webSite_cd = 1;
			}
			
			//Think商城
			String [] ThinkString = {"thinkpad.lenovo.com.cn","www.thinkworldshop.com.cn","cart.thinkworldshop.com.cn","order.thinkworldshop.com.cn","i.thinkworldshop.com.cn","mobile.thinkworldshop.com.cn","m.cart.thinkworldshop.com.cn","s.thinkworldshop.com.cn","coupon.thinkworld.com.cn","m.coupon.thinkworldshop.com.cn","m.order.thinkworldshop.com.cn"};
			List<String> ThinkList = Arrays.asList(ThinkString);
			if (ThinkList.contains(ps) || 
					(ps.equals("pay.i.lenovo.com") && (pu.contains("plat=5")||pu.contains("tkPc_url")||pu.contains("plat=8")||pu.contains("tkWap_url")))
					|| (ps.equals("c.lenovo.com.cn") && pu.contains("think"))
					|| (pu.toLowerCase().contains("shopid=2"))
					){
					webSite_cd = 2;
			}
			
			//EPP商城
			String [] EPPString = {"www.lenovovip.com.cn","cart.lenovovip.com.cn","member.lenovo.com.cn","s.lenovovip.com.cn","i.lenovovip.com.cn","order.lenovovip.com.cn","coupon.lenovovip.com.cn","m.order.lenovovip.com.cn","m.lenovovip.com.cn","m.cart.lenovovip.com.cn","m.coupon.lenovovip.com.cn"};
			List<String> EPPList = Arrays.asList(EPPString);
			if (EPPList.contains(ps) || 
					(ps.equals("pay.i.lenovo.com") && (pu.contains("plat=22")||pu.contains("plat=22")||pu.contains("plat=20")||pu.contains("eppWap_url")))
					|| (pu.toLowerCase().contains("shopid=3"))
					){
					webSite_cd = 3;
			}
			
			//Think官网
			if (ps.contains("thinkworld.com.cn")){
				webSite_cd = 7;
			}
			
			//联想商城老官网
			if (ps.equals("appserver.lenovo.com.cn")){
				webSite_cd = 8;
			}
			
			//SMB商城
			if (ps.contains("17.lenovo.com.cn") || pu.toLowerCase().contains("shopid=8")){
				webSite_cd = 13;
			}
			
			//smb积分商城
			if (ps.contains("17jf.lenovo.com.cn") || pu.toLowerCase().contains("shopid=9")){
				webSite_cd = 15;
			}
			
			//17ask论坛
			if (ps.contains("17ask.lenovo.com.cn") || pu.toLowerCase().contains("shopid=10")){
				webSite_cd = 16;
			}
			
		}else if (webSite.equals("10000008")){
			//联想社区2.0
			String [] Club2String = {"club.lenovo.com.cn","club.lenovo.cn","mall.club.lenovo.com.cn","mall.club.lenovo.cn","fans.lenovo.com.cn","bbs.vibeui.com","newclub.lenovo.cn"};
			List<String> Club2List = Arrays.asList(Club2String);
			if (Club2List.contains(ps)){
					webSite_cd = 9;
			}
			
			//Think社区2.0
			if (ps.contains("bbs.thinkworldshop.com.cn") || ps.contains("thinkbbs.lenovo.com.cn")){
				webSite_cd = 10;
			}
			
			
		}else if (webSite.equals("10000042")){
			//联想社区3.0
			String [] Club2String = {"club.lenovo.com.cn","club.lenovo.cn","mall.club.lenovo.com.cn","mall.club.lenovo.cn","fans.lenovo.com.cn","bbs.vibeui.com","newclub.lenovo.cn"};
			List<String> Club2List = Arrays.asList(Club2String);
			if (Club2List.contains(ps)){
					webSite_cd = 11;
			}
			
			//Think社区3.0
			if (ps.contains("bbs.thinkworldshop.com.cn") || ps.contains("thinkbbs.lenovo.com.cn")){
				webSite_cd = 12;
			}
		}else if(webSite.equals("10000071")){
			webSite_cd = 14;
		}
		return String.valueOf(webSite_cd);
	}
	
	public static String getTerminal_cd(String webSite, String ps, String pu, String sourcetypes){
		Integer Terminal_cd = 99;
		
			//PC
		String [] PCString = {"www.lenovo.com.cn","cart.lenovo.com.cn","order.lenovo.com.cn","i.lenovo.com.cn","coupon.lenovo.com.cn","www.thinkworldshop.com.cn","i.thinkworldshop.com.cn","cart.thinkworldshop.com.cn","order.thinkworldshop.com.cn","coupon.thinkworld.com.cn","www.lenovovip.com.cn","cart.lenovovip.com.cn","member.lenovo.com.cn","i.lenovovip.com.cn","order.lenovovip.com.cn","coupon.lenovovip.com.cn","www.thinkworld.com.cn","products.thinkworld.com.cn","appserver.lenovo.com.cn","club.lenovo.com.cn","mall.club.lenovo.com.cn","fans.lenovo.com.cn","bbs.vibeui.com","bbs.thinkworldshop.com.cn","c.lenovo.com.cn","support.lenovo.com.cn","17.lenovo.com.cn","reg.lenovo.com.cn","bbs.lenovo.com","support1.lenovo.com.cn","think.lenovo.com.cn","care.lenovo.com.cn","think.lcf5.lenovo.com.cn","cs.lenovo.com.cn"};
		List<String> PCList = Arrays.asList(PCString);
		String[] SString = {"s.lenovo.com.cn","s.thinkworldshop.com.cn","s.lenovovip.com.cn"};
		List<String> SList = Arrays.asList(SString);
		if ((PCList.contains(ps)  
				||(SList.contains(ps) && !pu.contains("/m/") && !pu.contains("/app/"))
				||(ps.equals("pay.i.lenovo.com") && (pu.contains("plat=4") ||pu.contains("Pc_url") ||pu.contains("plat=8") ||pu.contains("plat=22")))
				||(ps.equals("member.lenovo.com.cn") && !pu.contains("iswap=0"))
				||(ps.equals("z.lenovo.com.cn") && !pu.contains("wap"))
				||(pu.contains("terminal=1"))
				||(ps.contains("thinkworld.com.cn"))
				||(ps.contains("17.lenovo.com.cn"))
				||(ps.contains("17jf.lenovo.com.cn"))
				||(ps.contains("17ask.lenovo.com.cn"))
				||(webSite.equals("10000071")))
				&& (!pu.contains("/android/"))
				&& (!pu.contains("/weixin/"))
				&& !(sourcetypes.equals("android") || sourcetypes.equals("IOS"))
				){
				Terminal_cd = 1;
			
		}
			//WAP端
		String [] WAPString = {"m.lenovo.com.cn","m.cart.lenovo.com.cn","3g.lenovo.com.cn","mobile.thinkworldshop.com.cn","m.cart.thinkworldshop.com.cn","m.lenovovip.com.cn","m.cart.lenovovip.com.cn","club.lenovo.cn","mall.club.lenovo.cn","thinkbbs.lenovo.com.cn","m.order.thinkworldshop.com.cn","m.coupon.lenovo.com.cn","m.order.lenovo.com.cn","m.order.lenovovip.com.cn"};
		List<String> WAPList = Arrays.asList(WAPString);
		
		if ((WAPList.contains(ps)  
				||(SList.contains(ps) && pu.contains("/m/"))
				||(ps.equals("pay.i.lenovo.com") && (pu.contains("plat=1") ||pu.contains("Wap_url") ||pu.contains("plat=5") ||pu.contains("plat=20")))
				||(ps.equals("member.lenovo.com.cn") && pu.contains("iswap=0"))
				||(ps.equals("z.lenovo.com.cn") && pu.contains("wap"))
				||(pu.contains("terminal=2")))
				&& (!pu.contains("/weixin/"))
				&& !(sourcetypes.equals("android") || sourcetypes.equals("IOS"))
				){
				Terminal_cd = 2;
		}
			
			//APP端
		if ((webSite.equals("10000001")
				&& ((sourcetypes.equals("android") || sourcetypes.equals("IOS")) 
	            ||(SList.contains(ps) && pu.contains("/app/"))
	            ||pu.contains("terminal=3"))
				)){
			Terminal_cd = 3;
		}
		
			//微信端
		if ((webSite.equals("10000001"))
				&&((ps.equals("pay.i.lenovo.com") && pu.contains("plat=2"))
                ||(pu.contains("/weixin/"))
                ||(pu.contains("terminal=4")))
				){
			Terminal_cd = 4;
		}

		
		return String.valueOf(Terminal_cd);
	}


	public static String getReftype(String fs, String ps,  String website_cd, String kw, String msch){
		String refType = "4";
		
		//广告入站
		if (ps.contains("pmf_source")){
			refType = "1";
			
		//搜索入站
		}else if ((!kw.trim().isEmpty()) || msch.contains("free_search")){
			refType = "2";
			
		}
		
		//来源域名和受访域名相同时为直接入站
		if (ps.equals(fs)){
			refType = "3";
		}else if (website_cd.equals("1")){
			
			String [] NoShopString = {"appserver.lenovo.com.cn","think.lenovo.com.cn","support.lenovo.com.cn", "support1.lenovo.com.cn", "club.lenovo.com.cn"};
			List<String> NoShopList = Arrays.asList(NoShopString);
			
			//受访域名包含lenovo.com.cn+来源域名包含lenovo.com.cn -> 直接入站
			//受访域名包含lenovo.com.cn+来源域名包含lenovovip.com.cn -> 直接入站
			 if ((fs.contains("lenovo.com.cn") && ps.contains("lenovo.com.cn")) ||
					 (ps.contains("lenovo.com.cn") && fs.contains("lenovovip.com.cn"))){
					refType = "3";
			}
			 
			//服务(appserver.lenovo.com.cn等等)、社区的单独处理(club.lenovo.com.cn) -> 网站入站
			if (NoShopList.contains(fs)){
				refType = "4";
			}
		}else if (website_cd.equals("9")){
			if (fs.contains("mall.club.lenovo.com.cn") && fs.contains("thinkworld.com.cn")){
				refType = "3";
			}
		}else if (website_cd.equals("7")){
			if (ps.contains("thinkwold.com.cn") && fs.contains("thinkworld.com.cn")){
				refType = "3";
			}
		}else if (website_cd.equals("14")){
			String [] ServiceString = {"think.lenovo.com.cn","support.lenovo.com.cn", "support1.lenovo.com.cn"};
			List<String> ServiceList = Arrays.asList(ServiceString);
			if (ServiceList.contains(fs) && ServiceList.contains(ps)){
				refType = "3";
			}
		}
		
		return refType;
	}
}

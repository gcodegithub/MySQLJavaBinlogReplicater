package cn.ce.web.rest.i;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import cn.ce.binlog.vo.ControlBinlogXmlVO;
import cn.ce.binlog.vo.TokenAuthRes;

@Path(value = "/")
public interface IFBinlogService {

	@GET
	@Path(value = "/getToken")
	TokenAuthRes getToken(@QueryParam("slaveId") String slaveId,
			@QueryParam("tokenInput") String tokenInput);

	@GET
	@Path(value = "/startBinlogXML")
	public ControlBinlogXmlVO startBinlogXML();

	@GET
	@Path(value = "/stopBinlogXML")
	public ControlBinlogXmlVO stopBinlogXML() throws InterruptedException;
}

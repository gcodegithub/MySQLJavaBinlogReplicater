package cn.ce.web.rest.i;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.TokenAuthRes;

@Path(value = "/")
public interface IFBinlogService {

	@GET
	@Path(value = "/getToken")
	TokenAuthRes getToken(@QueryParam("slaveId") String slaveId,
			@QueryParam("tokenInput") String tokenInput);

	@GET
	@Path(value = "/getDump")
	BinParseResultVO getDump(@QueryParam("slaveId") String slaveId,
			@QueryParam("binlogfilename") String binlogfilename,
			@QueryParam("binlogPosition") String binlogPosition,
			@QueryParam("serverhost") String serverhost,
			@QueryParam("serverPort") String serverPort,
			@QueryParam("username") String username,
			@QueryParam("password") String password,
			@QueryParam("tokenInput") String tokenInput);

}

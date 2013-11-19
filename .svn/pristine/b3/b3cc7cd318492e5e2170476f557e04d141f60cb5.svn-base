package cn.ce.web.rest.i;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import cn.ce.web.rest.vo.OpParseResultVO;
import cn.ce.web.rest.vo.TokenAuthRes;

@Path(value = "/")
public interface IFOplogService {

	@GET
	@Path(value = "/getToken")
	TokenAuthRes getToken(@QueryParam("slaveId") String slaveId,
			@QueryParam("tokenInput") String tokenInput);

	@GET
	@Path(value = "/getDump")
	OpParseResultVO getDump(@QueryParam("slaveId") String slaveId,
			@QueryParam("oplogts") String binlogfilename,
			@QueryParam("serverhost") String serverhost,
			@QueryParam("serverPort") String serverPort,
			@QueryParam("username") String username,
			@QueryParam("password") String password,
			@QueryParam("tokenInput") String tokenInput);

}

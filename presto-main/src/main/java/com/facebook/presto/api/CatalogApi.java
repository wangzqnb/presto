package com.facebook.presto.api;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.PrestoServer;
import org.json.simple.JSONObject;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * 动态catalog api
 *
 * @author Zhaoqi Wang
 * @date 2021/9/11 23:58
 */
@Path("/presto/catalog/api")
public class CatalogApi {

    private static final Logger log = Logger.get(CatalogApi.class);

    private static final String BASE_DIR = "etc/catalog/";
    private static final Integer SUCCESS = 0;
    private static final Integer ERROR = -1;
    private static final Integer UNKNOWN = -2;


    /**
     * 增加catalog
     */
    @POST
    @Path("/add")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response add(JSONObject json) {
        return Response.ok(addMode(json)).build();
    }

    /**
     * 删除catalog
     */
    @POST
    @Path("/delete")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response delete(JSONObject json) {
        return Response.ok(deleteMode(json)).build();
    }


    /**
     * 更新catalog
     */
    @POST
    @Path("/update")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response update(JSONObject json) {
        if (deleteMode(json).equals(SUCCESS)) {
            return Response.ok(addMode(json)).build();
        }
        return Response.ok(ERROR).build();
    }

    /**
     * 判断catalog配置是否存在
     */
    @POST
    @Path("/conf")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response judgeCatalogIsExit(JSONObject json) {
        String catalogName = json.get("catalogName").toString();
        if (PrestoServer.isExist(catalogName)) {
            return Response.ok(SUCCESS).build();
        }
        return Response.ok(ERROR).build();
    }

    /**
     * 判断catalog文件是否存在
     */
    @POST
    @Path("/file")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response judgeCatalogFileIsExit(JSONObject json) {
        String catalogName = json.get("catalogName") + ".properties";
        File file = new File(BASE_DIR + catalogName);
        if (file.exists()) {
            return Response.ok(SUCCESS).build();
        }
        return Response.ok(ERROR).build();
    }

    /**
     * 先判断catalog配置是否存在
     * 如果不存在，判断配置文件是否存在
     */
    @POST
    @Path("/catalog")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response judgeCatalog(JSONObject json) {
        String catalogName = json.get("catalogName").toString();
        String catalogFileName = json.get("catalogName") + ".properties";
        File file = new File(BASE_DIR + catalogFileName);
        if (PrestoServer.isExist(catalogName)) {
            return Response.ok(SUCCESS).build();
        } else if (file.exists()) {
            return Response.ok(UNKNOWN).build();
        }
        return Response.ok(ERROR).build();
    }


    /**
     * add catalog
     * <p>
     * json样例数据
     * catalogName 必须存在，其他按照catalog格式key v添加
     * {
     * "catalogName":"mysql66",
     * "connector.name":"mysql",
     * "connection-url":"jdbc:mysql://localhost:3306",
     * "connection-user":"root",
     * "connection-password":"123456"
     * }
     *
     * @param json json数据
     * @return json code 0，success，-1，error
     */
    public static Integer addMode(JSONObject json) {
        StringJoiner sj = new StringJoiner("\n");
        json.keySet().stream().filter(item -> !"catalogName".equals(item)).forEach(item -> {
            StringJoiner joiner = new StringJoiner("=");
            String v = (String) json.get(item);
            joiner.add(item.toString()).add(v);
            sj.add(joiner.toString());
        });

        String catalogNameOld = json.get("catalogName") + ".bak";
        String catalogNameNew = json.get("catalogName") + ".properties";

        File file = new File(BASE_DIR + catalogNameOld);
        File tar = new File(BASE_DIR + catalogNameNew);
        if (!tar.exists()) {
            FileWriter writer = null;
            try {
                file.createNewFile();
                writer = new FileWriter(file, false);
                writer.append(sj.toString());
                writer.flush();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                return ERROR;
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        log.error(e.getLocalizedMessage());
                    }
                }
                //文件重命名
                file.renameTo(tar);
            }
            return SUCCESS;
        }
        return ERROR;
    }

    /**
     * delete catalog
     * <p>
     * json样例数据
     * catalogName 必须存在
     * {
     * "catalogName":"mysql66"
     * }
     *
     * @param json json数据
     * @return json code 0，success，-1，error
     */
    public Integer deleteMode(JSONObject json) {
        String catalogName = json.get("catalogName") + ".properties";
        File tar = new File(BASE_DIR + catalogName);
        if (tar.exists()) {
            tar.delete();
            return SUCCESS;
        }
        return ERROR;
    }


}

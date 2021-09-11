package com.facebook.presto.api;

import com.facebook.airlift.log.Logger;
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


    @POST
    @Path("/add")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response add(JSONObject json) {
        return Response.ok(addMode(json)).build();
    }

    @POST
    @Path("/delete")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response delete(JSONObject json) {
        return Response.ok(deleteMode(json)).build();
    }


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

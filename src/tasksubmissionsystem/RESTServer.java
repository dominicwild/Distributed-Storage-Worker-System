package tasksubmissionsystem;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.POST;
import javax.ws.rs.Consumes;
import com.sun.jersey.multipart.FormDataParam;
import com.sun.jersey.core.header.FormDataContentDisposition;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 *
 * @author Dominic
 */
@Path("/files")
public class RESTServer {

    //Constants relating to RMI and folders.
    private static final String FILES_FOLDER = "webapps/myapp/files/";
    private static final String HTML_FOLDER = "webapps/myapp/";
    private static final String RMI_MANAGEMENT_SERVER_URL = "rmi://localhost:1099/RESTManagement";

    @GET
    @Path("/{param}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM})
    public Response getFile(@PathParam("param") String filename) {

        //by default files are looked for in Tomcat's root directory, so we prepend our subdirectory on there first...
        filename = FILES_FOLDER + filename;

        //read filename into a byte array & send to client
        File f = new File(filename);

        if (f.exists()) {
            return Response.status(Response.Status.OK).entity(f).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).entity("").build();
        }
    }

    /**
     * Converts a passed input stream to a byte array.
     *
     * @param in InputStream to convert.
     * @return The byte array backing the InputStream.
     */
    public static byte[] inputStreamToByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final int BUF_SIZE = 1024;
        byte[] buffer = new byte[BUF_SIZE];
        int bytesRead = -1;
        while ((bytesRead = in.read(buffer)) > -1) {
            out.write(buffer, 0, bytesRead);
        }
        in.close();
        byte[] byteArray = out.toByteArray();
        return byteArray;
    }

    //this method gets called when a POST is sent to /base/files/newFile
    // - it expects multipart form data as the body of the POST request
    // - the fields of the form are mapped to the parameters of the method as specified below (i.e. FormParam() identifies a particular form field, the value of which goes into the method parameter)
    // - NOTE: the parameter "@FormDataParam("content") FormDataContentDisposition fileDetail" is optional, and can be used to later do e.g. fileDetail.getFileName()...
    @POST
    @Path("/newFile")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response newFile(@FormDataParam("name") String fileName, @FormDataParam("content") InputStream contentStream, @FormDataParam("content") FormDataContentDisposition fileDetail) throws IOException {

        //by default files are looked for in Tomcat's root directory, so we prepend our subdirectory on there first...
        fileName = FILES_FOLDER + fileName;
        //Write bytes to file on the system.
        byte contentBytes[] = inputStreamToByteArray(contentStream);
        File file = new File(fileName);
        FileOutputStream stream = new FileOutputStream(file);
        stream.write(contentBytes);
        stream.close();

        return Response.status(Response.Status.OK).entity(file.getAbsoluteFile().toString()).build();
    }

    /**
     * Gets the Remote RMI object to the management server to submit a task for
     * processing.
     *
     * @return The Remote interface object used to submit tasks for processing.
     */
    public RESTInterface getRESTServer() throws NotBoundException, MalformedURLException, RemoteException {
        return (RESTInterface) Naming.lookup("rmi://localhost:1099/RESTManagement");
    }

    /**
     * Puts a task within the system using the management server. Only called
     * from the REST interface.
     *
     * @param fileName The name of the file to process.
     * @param contentStream The bytes of the file.
     * @param type The type of processing to be done.
     * @return A HTML page on the outcome of the task submission.
     */
    @POST
    @Path("/putTask")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putTask(@FormDataParam("name") String fileName, @FormDataParam("content") InputStream contentStream, @FormDataParam("workType") String type) throws IOException, NotBoundException {
        byte contentBytes[] = inputStreamToByteArray(contentStream);
        RESTInterface server = this.getRESTServer();
        server.putTask(fileName, contentBytes, type);

        return Response.status(Response.Status.OK).entity(new File(HTML_FOLDER + "formOk.html")).build();
    }

    /**
     * Gets the results of a processed task and returns the XML result file.
     *
     * @param fileName The name of the results file to find.
     * @param taskType The type of task that was carried out.
     * @return The XML document as a Response object.
     */
    @GET
    @Path("/Results/{taskType}/{fileName}")
    @Produces(MediaType.TEXT_XML)
    public Response results(@PathParam("fileName") String fileName, @PathParam("taskType") String taskType) throws NotBoundException, MalformedURLException, RemoteException {
        byte[] results = this.getRESTServer().getResults(fileName, taskType);
        return Response.status(Response.Status.OK).entity(results).build();
    }

    /**
     * Lists all the ongoing and finished tasks within the system.
     *
     * @return A HTML page listing all the tasks in the system.
     */
    @GET
    @Path("/List")
    @Produces(MediaType.TEXT_HTML)
    public Response results() throws NotBoundException, MalformedURLException, RemoteException {
        String list = this.getRESTServer().fileList();
        return Response.status(Response.Status.OK).entity(list).build();
    }
}

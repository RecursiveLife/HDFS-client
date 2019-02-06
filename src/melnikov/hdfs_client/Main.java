package melnikov.hdfs_client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

public class Main {

    private java.nio.file.Path localPath;
    private Path hdfsPath;
    private URI hdfsUri;
    private Configuration configuration;
    private FileSystem fileSystem;

    public Main() {

    }

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

        if (args.length != 3) {
            System.out.println("Usage: java -jar HDFSclient.jar host port username");
            System.exit(1);
        }

        Main client = new Main();
        client.configuration = new Configuration();
        client.configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        client.configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        client.configuration.set("dfs.replication", "1");
        //client.configuration.set("fs.default.name", "hdfs://" + args[0] + ":" + args[1]);
        client.hdfsUri = new URI("hdfs://" + args[0] + ":" + args[1]);
        client.fileSystem = FileSystem.get(client.hdfsUri, client.configuration, args[2]);
        //client.fileSystem = FileSystem.get(client.configuration);
        client.hdfsPath = client.fileSystem.getHomeDirectory();
        //client.fileSystem.close();
        client.localPath = java.nio.file.Paths.get(System.getProperty("user.home"));
        Scanner scanner = new Scanner(System.in);
        String help = "\nWhat would you like to do? Options:\n"
                + " mkdir \"catalog name\" (создание каталога в HDFS)\n"
                + " put \"local file name\" (загрузка файла в HDFS)\n"
                + " get \"HDFS file name\" (скачивание файла из HDFS)\n"
                + " append \"local file name\" \"HDFS file name\" (конкатенация файла в HDFS с локальным файлом)\n"
                + " delete \"HDFS file name\" (удаление файла в HDFS)\n"
                + " ls (отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов)\n"
                + " cd \"catalog name\" (переход в другой каталог в HDFS, \"..\" - на уровень выше)\n"
                + " lls (отображение содержимого текущего локального каталога с разделением файлов и каталогов)\n"
                + " lcd \"local catalog name\" (переход в другой локальный каталог, \"..\" - на уровень выше)\n"
                + " help (отобразить эту подсказку еще раз)\n"
                + " exit (выйти из программы)";
        System.out.println(help);

        while(true) {

            System.out.print("> ");
            String input = scanner.nextLine();
            String[] splittedInput = client.splitInput(input);

            if (splittedInput[0].equals("mkdir")) {
                if (splittedInput.length < 2)
                    System.out.println("Wrong input!");
                else {
                    client.mkdir(new Path(client.hdfsPath, splittedInput[1]), client.fileSystem);
                }

            } else if (splittedInput[0].equals("put")) {
                if (splittedInput.length < 2)
                    System.out.println("Wrong input!");
                else {
                    client.addFile(java.nio.file.Paths.get(client.localPath.toString(), splittedInput[1]), client.hdfsPath, client.fileSystem);
                }

            } else if (splittedInput[0].equals("get")) {
                if (splittedInput.length < 2)
                    System.out.println("Wrong input!");
                else {
                    client.readFile(new Path(client.hdfsPath, splittedInput[1]), client.localPath, client.fileSystem);
                }

            } else if (splittedInput[0].equals("append")) {
                if (splittedInput.length < 3)
                    System.out.println("Wrong input!");
                else {
                    client.appendFile(java.nio.file.Paths.get(client.localPath.toString(), splittedInput[1]), new Path(client.hdfsPath, splittedInput[2]), client.fileSystem);
                }

            } else if (splittedInput[0].equals("delete")) {
                if (splittedInput.length < 2)
                    System.out.println("Wrong input!");
                else {
                    client.deleteFile(new Path(client.hdfsPath, splittedInput[1]), client.fileSystem);
                }

            } else if (splittedInput[0].equals("ls")) {
                client.ls(client.hdfsPath, client.fileSystem);

            } else if (splittedInput[0].equals("cd")) {
                if (splittedInput.length < 2)
                    client.cd(client.hdfsPath, "", client.fileSystem);
                else
                    client.hdfsPath = client.cd(client.hdfsPath, splittedInput[1], client.fileSystem);

            } else if (splittedInput[0].equals("lls")) {
                client.lls(client.localPath);

            } else if (splittedInput[0].equals("lcd")) {
                if (splittedInput.length < 2)
                    client.lcd(client.localPath, "");
                else
                    client.localPath = client.lcd(client.localPath, splittedInput[1]);

            } else if (splittedInput[0].equals("help"))
                System.out.println(help);

            else if (splittedInput[0].equals("exit"))
                break;

            else
                System.out.println("Did not recognised command.");

        }
        scanner.close();
        client.fileSystem.close();
    }

    private String[] splitInput(String input) {

        if (!input.contains("\""))
            return new String[]{input};

        int index1 = input.indexOf("\"") + 1;
        int index2 = input.indexOf("\"", index1);

        if (!input.substring(index2 + 1).contains("\"")) {

            return new String[]{
                    input.substring(0, index1 - 2),
                    input.substring(index1, index2)};

        } else {

            int index3 = input.indexOf("\"", index2 + 1) + 1;
            int index4 = input.indexOf("\"", index3);

            return new String[]{
                    input.substring(0, index1 - 2),
                    input.substring(index1, index2),
                    input.substring(index3, index4)};

        }
    }

    public void mkdir(Path path, FileSystem fileSystem) throws IOException {

        if (fileSystem.exists(path)) {
            System.out.println("Dir " + path + " already exists");
            return;
        }

        fileSystem.mkdirs(path);

    }

    public void addFile(java.nio.file.Path source, Path dest, FileSystem fileSystem) throws IOException {

        Path path = new Path(dest, source.getFileName().toString());
        if (!source.toFile().exists()) {
            System.out.println("File " + source + " does not exists");
            return;
        }
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(source.toFile()));

        byte[] b = new byte[1024];
        int numBytes;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
    }

    public void readFile(Path source, java.nio.file.Path dest, FileSystem fileSystem) throws IOException {

        if (!fileSystem.exists(source)) {
            System.out.println("File " + source + " does not exists");
            return;
        }

        String filename = source.getName();
        dest = java.nio.file.Paths.get(dest.toString(), filename);

        if (dest.toFile().exists()) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(source);

        OutputStream out = new BufferedOutputStream(new FileOutputStream(dest.toFile()));

        byte[] b = new byte[1024];
        int numBytes;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
    }

    public void appendFile(java.nio.file.Path source, Path dest, FileSystem fileSystem) throws IOException, InterruptedException {

        if(!source.toFile().exists()) {
            System.out.println("File " + source + " does not exist");
            return;
        }

        if (!fileSystem.exists(dest)) {
            System.out.println("File " + dest + " does not exist");
            return;
        }

        //fileSystem.setReplication(dest, (short)1);
        fileSystem.setReplication(dest, fileSystem.getDefaultReplication(dest));

        FSDataOutputStream out = fileSystem.append(dest);
        InputStream in = new BufferedInputStream(new FileInputStream(source.toFile()));

        byte[] b = new byte[1024];
        int numBytes;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
    }

    public void deleteFile(Path file, FileSystem fileSystem) throws IOException {

        if (!fileSystem.exists(file)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(file, true);

    }

    public void ls(Path path, FileSystem fileSystem) throws IOException {

        if (!fileSystem.isDirectory(path)) {
            System.out.println("Path " + path + " is not a directory");
            return;
        }

        for (FileStatus fs: fileSystem.listStatus(path))
            if(fs.isDirectory())
                System.out.println("    \uD83D\uDCC1 " + fs.getPath().getName());

        for (FileStatus fs: fileSystem.listStatus(path))
            if(fs.isSymlink())
                System.out.println("    \uD83D\uDCC2 " + fs.getPath().getName());

        for (FileStatus fs: fileSystem.listStatus(path))
            if(fs.isFile())
                System.out.println("    \uD83D\uDCC4 " + fs.getPath().getName());

        for (FileStatus fs: fileSystem.listStatus(path))
            if(!fs.isDirectory() && !fs.isFile() && !fs.isSymlink())
                System.out.println("    This is neither file or directory: " + fs.getPath().getName());

    }

    public Path cd(Path path, String input, FileSystem fileSystem) throws IOException {

        if(input.isEmpty()) {
            System.out.println(path.toString());
            return path;
        }

        Path resultPath = new Path(path, input);
        if(fileSystem.isDirectory(resultPath))
            return resultPath;

        resultPath = new Path(input);
        if(resultPath.isAbsolute())
            return resultPath;

        System.out.println("Something wrong with input");
        return path;
    }

    public void lls(java.nio.file.Path filepath) {

        File file = filepath.toFile();

        for (File f: file.listFiles())
            if( f.isDirectory() )
                System.out.println("    \uD83D\uDCC1 " + f.getName());

        for (File f: file.listFiles())
            if( f.isFile() )
                System.out.println("    \uD83D\uDCC4 " + f.getName());

        for (File f: file.listFiles())
            if( !f.isDirectory() && !f.isFile() )
                System.out.println("    This is neither file or directory: " + f.getName());

    }

    public java.nio.file.Path lcd(java.nio.file.Path sPath, String input) {
        if(input.isEmpty()) {
            System.out.println(sPath);
            return sPath;
        }

        java.nio.file.Path tempPath;
        if(input.substring(0,2).matches("[a-zA-Z]:")) {
            tempPath = java.nio.file.Paths.get(input).normalize();
            return tempPath;
        }
        tempPath = java.nio.file.Paths.get(sPath.toString(), input);
        if( tempPath.toFile().isDirectory() )
            return tempPath.normalize();
        tempPath = java.nio.file.Paths.get(input);
        if( tempPath.toFile().isDirectory() )
            return tempPath.normalize();
        System.out.println("Something is wrong with input");
        return sPath;
    }

}

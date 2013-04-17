package edu.uci.ics.asterix.installer.test;

import java.io.File;
import java.io.FilenameFilter;

public class AsterixInstallerIntegrationUtil {

    private static String managixHome;

    public static void deinit() {

    }

    public static void init() {
        // TODO Auto-generated method stub
        File asterixProjectDir = new File(System.getProperty("user.dir"));
        File installerTargetDir = new File(asterixProjectDir, "target");
        System.out.println("asterix project dir" + asterixProjectDir.getAbsolutePath());
        System.out.println("installer target dir" + installerTargetDir.getAbsolutePath());
        String managixHomeDirName = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }

        })[0];
        managixHome = new File(installerTargetDir, managixHomeDirName).getAbsolutePath();
        System.out.println("Setting managix home to :" + managixHome);
    }

    public static String getManagixHome() {
        return managixHome;
    }

    public static void main(String [] args){
        init();
    }
}

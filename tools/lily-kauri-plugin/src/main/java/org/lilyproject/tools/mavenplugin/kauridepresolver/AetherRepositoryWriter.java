/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.mavenplugin.kauridepresolver;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.util.layout.MavenDefaultLayout;
import org.sonatype.aether.util.layout.RepositoryLayout;

public class AetherRepositoryWriter {

    public static void write(Set<Artifact> artifacts, String targetDirectory) throws MojoExecutionException {
        RepositoryLayout m2layout = new MavenDefaultLayout();

        for (Artifact artifact : artifacts) {
            File src = artifact.getFile();
            File dest = new File(targetDirectory, m2layout.getPath(artifact).getPath());
            try {
                System.out.println("Copying " + src);
                FileUtils.copyFile(src, dest);
            } catch (IOException e) {
                throw new MojoExecutionException("Error copying file " + src + " to " + dest);
            }

            // Kauri does not need the pom files, but let's copy them anyway, for informational purposes
            File srcPom = pomFile(src);
            File destPom = pomFile(dest);
            if (srcPom != null && srcPom.exists()) {
                try {
                    FileUtils.copyFile(srcPom, destPom);
                } catch (IOException e) {
                    throw new MojoExecutionException("Error copying file " + srcPom + " to " + destPom);
                }
            }
        }
    }

    public static File pomFile(File jarFile) throws MojoExecutionException {
        String path = jarFile.getAbsolutePath();

        if (!path.endsWith(".jar")) {
            return null;
        }

        return new File(path.replaceAll("\\.jar$", ".pom"));
    }
}

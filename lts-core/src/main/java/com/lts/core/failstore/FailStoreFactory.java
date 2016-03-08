package com.lts.core.failstore;

import com.lts.core.cluster.Config;
import com.lts.core.spi.SPI;
import com.lts.core.spi.SpiExtensionKey;

/**
 * Robert HG (254963746@qq.com) on 5/21/15.
 */
@SPI(key = SpiExtensionKey.FAIL_STORE, dftValue = "leveldb")
public interface FailStoreFactory {

    public FailStore getFailStore(Config config, String storePath);

}

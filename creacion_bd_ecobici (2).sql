CREATE SCHEMA IF NOT EXISTS `ecobici` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `ecobici` ;

-- -----------------------------------------------------
-- Table `ecobici`.`bici`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ecobici`.`bici` (
  `idBici` INT NOT NULL,
  PRIMARY KEY (`idBici`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


-- -----------------------------------------------------
-- Table `ecobici`.`estacion`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ecobici`.`estacion` (
  `idEstacion` INT NOT NULL,
  `nombre` VARCHAR(100) NULL DEFAULT NULL,
  `latitud` FLOAT NULL DEFAULT NULL,
  `longitud` FLOAT NULL DEFAULT NULL,
  `tipoEstacion` VARCHAR(13) NULL DEFAULT NULL,
  PRIMARY KEY (`idEstacion`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


-- -----------------------------------------------------
-- Table `ecobici`.`viaje`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ecobici`.`viaje` (
  `idViaje` INT NOT NULL,
  `generoUsuario` CHAR(1) NULL DEFAULT NULL,
  `edadUsuario` INT NULL DEFAULT NULL,
  `idBici` INT NULL DEFAULT NULL,
  `idEstacionRetiro` INT NULL DEFAULT NULL,
  `fechaRetiro` DATETIME NULL DEFAULT NULL,
  `idEstacionArribo` INT NULL DEFAULT NULL,
  `fechaArribo` DATETIME NULL DEFAULT NULL,
  PRIMARY KEY (`idViaje`),
  INDEX `fk_est_retiro_idx` (`idEstacionRetiro` ASC) ,
  INDEX `fk_est_arribo_idx` (`idEstacionArribo` ASC) ,
  INDEX `fk_bici_idx` (`idBici` ASC) ,
  CONSTRAINT `fk_bici`
    FOREIGN KEY (`idBici`)
    REFERENCES `ecobici`.`bici` (`idBici`),
  CONSTRAINT `fk_est_arribo`
    FOREIGN KEY (`idEstacionArribo`)
    REFERENCES `ecobici`.`estacion` (`idEstacion`),
  CONSTRAINT `fk_est_retiro`
    FOREIGN KEY (`idEstacionRetiro`)
    REFERENCES `ecobici`.`estacion` (`idEstacion`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


